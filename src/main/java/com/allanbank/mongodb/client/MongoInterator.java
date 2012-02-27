/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Iterator over the results of the MongoDB cursor.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoInterator implements ClosableIterator<Document> {

    /** The log for the iterator. */
    private static final Logger LOG = Logger.getLogger(MongoInterator.class
            .getName());

    /** The client for sending get_more requests to the server. */
    private final Client myClient;

    /** The iterator over the current set of documents. */
    private Iterator<Document> myCurrentIterator;

    /** The original query. */
    private long myCursorId = 0;

    /** The {@link Future} that will be updated with the next set of results. */
    private FutureCallback<Reply> myNextReply;

    /** The original query. */
    private final Query myOriginalQuery;

    /**
     * Create a new MongoDBInterator.
     * 
     * @param originalQuery
     *            The original query being iterated over.
     * @param client
     *            The client for issuing more requests.
     * @param reply
     *            The initial results of the query that are available.
     */
    public MongoInterator(final Query originalQuery, final Client client,
            final Reply reply) {
        myNextReply = new FutureCallback<Reply>();
        myNextReply.callback(reply);

        myCursorId = 0;
        myClient = client;
        myOriginalQuery = originalQuery;
        myCurrentIterator = null;
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
        final long cursorId = myCursorId;
        final Future<Reply> replyFuture = myNextReply;

        myCurrentIterator = null;
        myNextReply = null;
        myCursorId = 0;

        if (cursorId == 0) {
            // May not have processed any of the results yet...
            if (replyFuture != null) {
                try {
                    final Reply reply = replyFuture.get();

                    if (reply.getCursorId() != 0) {
                        myClient.send(new KillCursors(new long[] { reply
                                .getCursorId() }));
                    }
                }
                catch (final InterruptedException e) {
                    LOG.log(Level.WARNING,
                            "Intertrupted waiting for a query reply to close the cursor.",
                            e);
                }
                catch (final ExecutionException e) {
                    LOG.log(Level.WARNING,
                            "Intertrupted waiting for a query reply to close the cursor.",
                            e);
                }
            }
        }
        else {
            myClient.send(new KillCursors(new long[] { cursorId }));
        }
    }

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
     * Loads more documents into the iterator. This iterator issues a get_more
     * command as soon as the previous results start to be used.
     * 
     * @throws RuntimeException
     *             On a failure to load documents.
     */
    protected void loadDocuments() throws RuntimeException {
        try {
            // Pull the reply from the future. Hopefully it is already there!
            final Reply reply = myNextReply.get();
            myCursorId = reply.getCursorId();

            // Pre-fetch the next set of documents while we iterate over the
            // documents we just got.
            if (myCursorId != 0) {
                final GetMore getMore = new GetMore(
                        myOriginalQuery.getDatabaseName(),
                        myOriginalQuery.getCollectionName(), myCursorId,
                        myOriginalQuery.getNumberToReturn());

                myNextReply = new FutureCallback<Reply>();
                myClient.send(getMore, myNextReply);
            }
            else {
                // Exhausted the cursor - no more results.
                myNextReply = null;

                // Don't need to kill the cursor since we exhausted it.
            }

            myCurrentIterator = reply.getResults().iterator();
        }
        catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        catch (final ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
