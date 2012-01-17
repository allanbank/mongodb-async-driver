/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.messsage.GetMore;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Iterator over the results of the MongoDB cursor.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoInterator implements Iterator<Document> {

    /** The client for sending get_more requests to the server. */
    private final Client myClient;

    /** The iterator over the current set of documents. */
    private Iterator<Document> myCurrentIterator;

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

        myClient = client;
        myOriginalQuery = originalQuery;
        myCurrentIterator = null;
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
            // First call.
            final Reply reply = myNextReply.get();

            // Start the get_more while we iterate over the documents we
            // have.
            if (reply.getCursorId() != 0) {
                final GetMore getMore = new GetMore(
                        myOriginalQuery.getDatabaseName(),
                        myOriginalQuery.getCollectionName(),
                        reply.getCursorId(),
                        myOriginalQuery.getNumberToReturn());

                myNextReply = new FutureCallback<Reply>();
                myClient.send(getMore, myNextReply);
            }
            else {
                // Exhausted the cursor.
                myNextReply = null;
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
