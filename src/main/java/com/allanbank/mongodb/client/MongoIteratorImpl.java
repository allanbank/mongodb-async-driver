/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.CursorNotFoundException;

/**
 * Iterator over the results of the MongoDB cursor.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoIteratorImpl implements MongoIterator<Document> {

    /** The log for the iterator. */
    private static final Logger LOG = Logger.getLogger(MongoIteratorImpl.class
            .getName());

    /** The size of batches that are requested from the servers. */
    private int myBatchSize = 0;

    /** The client for sending get_more requests to the server. */
    private final Client myClient;

    /** The name of the collection the query was originally created on. */
    private final String myCollectionName;

    /** The iterator over the current set of documents. */
    private Iterator<Document> myCurrentIterator;

    /** The original query. */
    private long myCursorId = 0;

    /** The name of the database the query was originally created on. */
    private final String myDatabaseName;

    /**
     * The maximum number of document to return from the cursor. Zero or
     * negative means all.
     */
    private int myLimit = 0;

    /** The {@link Future} that will be updated with the next set of results. */
    private FutureCallback<Reply> myNextReply;

    /** The read preference to subsequent requests. */
    private final ReadPreference myReadPerference;

    /**
     * Flag to shutdown this iterator gracefully without closing the cursor on
     * the server.
     */
    private boolean myShutdown = false;

    /**
     * Create a new MongoIteratorImpl from a cursor document.
     * 
     * @param client
     *            The client interface to the server.
     * @param cursorDocument
     *            The original query.
     * 
     * @see MongoIteratorImpl#asDocument()
     */
    public MongoIteratorImpl(final Document cursorDocument, final Client client) {
        final String ns = cursorDocument.get(StringElement.class, "ns")
                .getValue();
        String db = ns;
        String collection = ns;
        final int index = ns.indexOf('.');
        if (0 < index) {
            db = ns.substring(0, index);
            collection = ns.substring(index + 1);
        }

        myClient = client;
        myDatabaseName = db;
        myCollectionName = collection;
        myCursorId = cursorDocument.get(NumericElement.class, "$cursor_id")
                .getLongValue();
        myLimit = cursorDocument.get(NumericElement.class, "$limit")
                .getIntValue();
        myBatchSize = cursorDocument.get(NumericElement.class, "$batch_size")
                .getIntValue();
        myReadPerference = ReadPreference.server(cursorDocument.get(
                StringElement.class, "$server").getValue());
    }

    /**
     * Create a new MongoDBInterator.
     * 
     * @param originalQuery
     *            The original query being iterated over.
     * @param client
     *            The client for issuing more requests.
     * @param server
     *            The server that received the original query request.
     * @param reply
     *            The initial results of the query that are available.
     */
    public MongoIteratorImpl(final Query originalQuery, final Client client,
            final String server, final Reply reply) {
        myNextReply = new FutureCallback<Reply>();
        myNextReply.callback(reply);

        myReadPerference = ReadPreference.server(server);
        myCursorId = 0;
        myClient = client;
        myCurrentIterator = null;
        myBatchSize = originalQuery.getBatchSize();
        myLimit = originalQuery.getLimit();
        myDatabaseName = originalQuery.getDatabaseName();
        myCollectionName = originalQuery.getCollectionName();

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the active cursor in the defined format.
     * </p>
     * 
     * @see ClientImpl#isCursorDocument(Document)
     */
    @Override
    public Document asDocument() {
        long cursorId = myCursorId;
        final Future<Reply> replyFuture = myNextReply;

        cursorId = retreiveCursorIdFromPendingRequest(cursorId, replyFuture);

        if (cursorId != 0) {
            final DocumentBuilder b = BuilderFactory.start();
            b.add("ns", myDatabaseName + "." + myCollectionName);
            b.add("$cursor_id", cursorId);
            b.add("$server", myReadPerference.getServer());
            b.add("$limit", myLimit);
            b.add("$batch_size", myBatchSize);

            return b.build();
        }

        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the iterator and send a {@link KillCursors} for the
     * open cursor, if any.
     * </p>
     */
    @Override
    public void close() {
        long cursorId = myCursorId;
        final Future<Reply> replyFuture = myNextReply;

        myCurrentIterator = null;
        myNextReply = null;
        myCursorId = 0;

        cursorId = retreiveCursorIdFromPendingRequest(cursorId, replyFuture);

        if ((cursorId != 0) && !myShutdown) {
            // The user asked us to leave the cursor be.
            myClient.send(new KillCursors(new long[] { cursorId },
                    myReadPerference), null);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to get the batch size from the original query or set
     * explicitly.
     * </p>
     */
    @Override
    public int getBatchSize() {
        return myBatchSize;
    }

    /**
     * Returns the iterator's read preference which points to the original
     * server performing the query.
     * 
     * @return The iterator's read preference which points to the original
     *         server performing the query.
     */
    public ReadPreference getReadPerference() {
        return myReadPerference;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true if there are more documents.
     * </p>
     */
    @Override
    public boolean hasNext() {
        if (myCurrentIterator == null) {
            loadDocuments();
        }
        else if (!myCurrentIterator.hasNext() && (myNextReply != null)) {
            loadDocuments();
        }
        return myCurrentIterator.hasNext();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return this iterator.
     * </p>
     */
    @Override
    public Iterator<Document> iterator() {
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the next document from the query.
     * </p>
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Document next() {
        if (hasNext()) {
            return myCurrentIterator.next();
        }
        throw new NoSuchElementException("No more documents.");
    }

    /**
     * Computes the size for the next batch of documents to get.
     * 
     * @return The returnNex
     */
    public int nextBatchSize() {
        if ((0 < myLimit) && (myLimit <= myBatchSize)) {
            return -myLimit;
        }
        return myBatchSize;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw and {@link UnsupportedOperationException}.
     * </p>
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Cannot remove a document via a MongoDB iterator.");
    }

    /**
     * Restarts the iterator by sending a request for more documents.
     * 
     * @throws MongoDbException
     *             On a failure to send the request for more document.
     */
    public void restart() throws MongoDbException {
        sendRequest();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the batch size.
     * </p>
     */
    @Override
    public void setBatchSize(final int batchSize) {
        myBatchSize = batchSize;
    }

    /**
     * Stops the iterator after consuming any received and/or requested batches.
     * <p>
     * <b>WARNING</b>: This will leave the cursor open on the server. Users
     * should persist the state of the cursor as returned from
     * {@link #asDocument()} and restart the cursor using one of the
     * {@link MongoClient#restart(com.allanbank.mongodb.bson.DocumentAssignable)}
     * or
     * {@link MongoClient#restart(com.allanbank.mongodb.StreamCallback, com.allanbank.mongodb.bson.DocumentAssignable)}
     * methods. Use with extreme caution.
     * </p>
     * <p>
     * The iterator will naturally stop ({@link #hasNext()} will return false)
     * when the current batch and any already requested batches are finished.
     * </p>
     */
    @Override
    public void stop() {
        myShutdown = true;
    }

    /**
     * Returns the client value.
     * 
     * @return The client value.
     */
    protected Client getClient() {
        return myClient;
    }

    /**
     * Returns the collection name.
     * 
     * @return The collection name.
     */
    protected String getCollectionName() {
        return myCollectionName;
    }

    /**
     * Returns the cursor Id value.
     * 
     * @return The cursor Id value.
     */
    protected long getCursorId() {
        return myCursorId;
    }

    /**
     * Returns the database name value.
     * 
     * @return The database name value.
     */
    protected String getDatabaseName() {
        return myDatabaseName;
    }

    /**
     * Returns the limit value.
     * 
     * @return The limit value.
     */
    protected int getLimit() {
        return myLimit;
    }

    /**
     * Loads more documents into the iterator. This iterator issues a get_more
     * command as soon as the previous results start to be used.
     * 
     * @throws RuntimeException
     *             On a failure to load documents.
     */
    protected void loadDocuments() throws RuntimeException {
        loadDocuments(true);
    }

    /**
     * Loads more documents into the iterator. This iterator issues a get_more
     * command as soon as the previous results start to be used.
     * 
     * @param blockForTailable
     *            If true then the method will recursively call itself on a
     *            tailable cursor with no results. This makes the call blocking.
     *            It false then the call will not block. This is used by the
     *            method to ensure that the outermost load blocks but the
     *            recursion is not inifinite.
     * @return The list of loaded documents.
     * 
     * @throws RuntimeException
     *             On a failure to load documents.
     */
    protected List<Document> loadDocuments(final boolean blockForTailable)
            throws RuntimeException {
        List<Document> docs;
        try {
            // Pull the reply from the future. Hopefully it is already there!
            final Reply reply = myNextReply.get();
            if (reply.isCursorNotFound() || reply.isQueryFailed()) {
                final long cursorid = myCursorId;
                myCursorId = 0;
                throw new CursorNotFoundException(reply, "Cursor id ("
                        + cursorid + ") not found by the MongoDB server.");
            }

            myCursorId = reply.getCursorId();

            // Setup and iterator over the documents and adjust the limit
            // for the documents we have. Do this before the fetch again
            // so the nextBatchSize() has the updated limit.
            docs = reply.getResults();
            myCurrentIterator = docs.iterator();
            if (0 < myLimit) {
                // Check if we have too many docs.
                if (myLimit <= docs.size()) {
                    myCurrentIterator = docs.subList(0, myLimit).iterator();
                    if (myCursorId != 0) {
                        // Kill the cursor.
                        myClient.send(new KillCursors(
                                new long[] { myCursorId }, myReadPerference),
                                null);
                        myCursorId = 0;
                    }
                }
                myLimit -= docs.size();
            }

            // Pre-fetch the next set of documents while we iterate over the
            // documents we just got.
            if ((myCursorId != 0) && !myShutdown) {
                sendRequest();

                // Include the (myNextReply != null) to catch failures on the
                // server.
                while (docs.isEmpty() && blockForTailable
                        && (myNextReply != null)) {
                    // Tailable - Wait for a reply with documents.
                    docs = loadDocuments(false);
                }
            }
            else {
                // Exhausted the cursor or are shutting down - no more results.
                myNextReply = null;

                // Don't need to kill the cursor since we exhausted it or are
                // shutting down.
            }

        }
        catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (final ExecutionException e) {
            throw new RuntimeException(e);
        }

        return docs;
    }

    /**
     * If the current cursor id is zero then waits for the response from the
     * pending request to determine the real cursor id.
     * 
     * @param cursorId
     *            The presumed cursor id.
     * @param replyFuture
     *            The pending reply's future.
     * @return The best known cursor id.
     */
    protected long retreiveCursorIdFromPendingRequest(final long cursorId,
            final Future<Reply> replyFuture) {
        // May not have processed any of the results yet...
        if ((cursorId == 0) && (replyFuture != null)) {
            try {
                final Reply reply = replyFuture.get();

                return reply.getCursorId();
            }
            catch (final InterruptedException e) {
                LOG.log(Level.WARNING,
                        "Intertrupted waiting for a query reply: "
                                + e.getMessage(), e);
            }
            catch (final ExecutionException e) {
                LOG.log(Level.WARNING,
                        "Intertrupted waiting for a query reply: "
                                + e.getMessage(), e);
            }
        }
        return cursorId;
    }

    /**
     * Sends a request for more documents.
     * 
     * @throws MongoDbException
     *             On a failure to send the request for more document.
     */
    protected void sendRequest() throws MongoDbException {
        final GetMore getMore = new GetMore(myDatabaseName, myCollectionName,
                myCursorId, nextBatchSize(), myReadPerference);

        myNextReply = new FutureCallback<Reply>();
        myClient.send(getMore, myNextReply);
    }
}
