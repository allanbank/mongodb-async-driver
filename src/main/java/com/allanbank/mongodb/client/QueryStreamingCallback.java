/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to convert a {@link Query} {@link Reply} into a series of callback
 * for each document received.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class QueryStreamingCallback extends
        AbstractValidatingReplyCallback {

    /** The server the original request was sent to. */
    private volatile String myAddress;

    /** The original query. */
    private final Client myClient;

    /** The original query. */
    private long myCursorId = 0;

    /** The callback to forward the returned documents to. */
    private final Callback<Document> myForwardCallback;

    /**
     * The maximum number of document to return from the cursor. Zero or
     * negative means all.
     */
    private int myLimit = 0;

    /** The original query. */
    private final Query myOriginalQuery;

    /** The server the original request was sent to. */
    private volatile Reply myReply;

    /**
     * Create a new QueryCallback.
     * 
     * @param client
     *            The client interface to the server.
     * @param originalQuery
     *            The original query.
     * @param results
     *            The callback to update with each document.
     */
    public QueryStreamingCallback(final Client client,
            final Query originalQuery, final Callback<Document> results) {

        myClient = client;
        myOriginalQuery = originalQuery;
        myForwardCallback = results;
        myLimit = originalQuery.getLimit();
    }

    @Override
    public void exception(final Throwable thrown) {
        close();
        synchronized (myForwardCallback) {
            myForwardCallback.exception(thrown);
        }
    }

    /**
     * Returns the server the original request was sent to.
     * 
     * @return The server the original request was sent to.
     */
    public String getAddress() {
        return myAddress;
    }

    /**
     * Sets the value of the server the original request was sent to.
     * 
     * @param address
     *            The new value for the server the original request was sent to.
     */
    public void setAddress(final String address) {
        myAddress = address;
        // For races make sure that the push has the server name.
        if (myReply != null) {
            final Reply reply = myReply;
            myReply = null;
            push(reply);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to add the {@link Query} to the exception.
     * </p>
     * 
     * @see AbstractReplyCallback#asError(Reply, int, int, String)
     */
    @Override
    protected MongoDbException asError(final Reply reply, final int okValue,
            final int errorNumber, final String errorMessage) {
        return new ReplyException(okValue, errorNumber, errorMessage,
                myOriginalQuery, reply);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to push the documents to the application's callback.
     * </p>
     * 
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected void handle(final Reply reply) throws MongoDbException {
        if (myAddress != null) {
            push(reply);
        }
        else {
            myReply = reply;
        }
    }

    /**
     * Loads more documents. This issues a get_more command as soon as the
     * previous results start to be used.
     * 
     * @param reply
     *            The last reply received.
     * @return The list of loaded documents.
     * 
     * @throws RuntimeException
     *             On a failure to load documents.
     */
    protected List<Document> loadDocuments(final Reply reply)
            throws RuntimeException {
        List<Document> docs = Collections.emptyList();
        myCursorId = reply.getCursorId();

        // Setup the documents and adjust the limit for the documents we have.
        // Do this before the fetch again so the nextBatchSize() has the updated
        // limit.
        docs = reply.getResults();
        if (0 < myLimit) {
            // Check if we have too many docs.
            if (myLimit <= docs.size()) {
                docs = docs.subList(0, myLimit);
                close();
            }
            myLimit -= docs.size();
        }

        // Pre-fetch the next set of documents while we iterate over the
        // documents we just got.
        if (myCursorId != 0) {
            final GetMore getMore = new GetMore(
                    myOriginalQuery.getDatabaseName(),
                    myOriginalQuery.getCollectionName(), myCursorId,
                    nextBatchSize(), ReadPreference.server(myAddress));

            myClient.send(getMore, this);
        }
        // Exhausted the cursor - no more results.
        // Don't need to kill the cursor since we exhausted it.

        return docs;
    }

    /**
     * Computes the size for the next batch of documents to get.
     * 
     * @return The returnNex
     */
    protected int nextBatchSize() {
        if ((0 < myLimit) && (myLimit <= myOriginalQuery.getBatchSize())) {
            return -myLimit;
        }
        return myOriginalQuery.getBatchSize();
    }

    /**
     * Overridden to close the iterator and send a {@link KillCursors} for the
     * open cursor, if any.
     */
    private void close() {
        final long cursorId = myCursorId;
        myCursorId = 0;

        if (cursorId != 0) {
            myClient.send(new KillCursors(new long[] { cursorId },
                    ReadPreference.server(myAddress)), null);
        }
    }

    /**
     * Pushes the results from the reply to the application's callback.
     * 
     * @param reply
     *            The reply containing the results to push to the application's
     *            callback.
     */
    private void push(final Reply reply) {
        synchronized (myForwardCallback) {
            // Request the load in the synchronized block so there is only 1
            // outstanding request.
            try {
                for (final Document result : loadDocuments(reply)) {
                    myForwardCallback.callback(result);
                }
                if (myCursorId == 0) {
                    // Signal the end of the results.
                    myForwardCallback.callback(null);
                }
            }
            catch (final RuntimeException re) {
                exception(re);
                close();
            }
        }
    }
}