/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to convert a {@link Query} {@link Reply} into a
 * {@link MongoInterator}.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class QueryCallback extends
        AbstractReplyCallback<ClosableIterator<Document>> {

    /** The original query. */
    private final Client myClient;

    /** The original query. */
    private final Query myQueryMessage;

    /**
     * Create a new QueryCallback.
     * 
     * @param client
     *            The client interface to the server.
     * @param queryMessage
     *            The original query.
     * @param results
     *            The callback to update once the first set of results are
     *            ready.
     */
    public QueryCallback(final Client client, final Query queryMessage,
            final Callback<ClosableIterator<Document>> results) {

        super(results);

        myClient = client;
        myQueryMessage = queryMessage;
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
                myQueryMessage, reply);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a {@link MongoInterator} around the reply.
     * </p>
     * 
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected ClosableIterator<Document> convert(final Reply reply)
            throws MongoDbException {
        return new MongoInterator(myQueryMessage, myClient, reply);
    }
}