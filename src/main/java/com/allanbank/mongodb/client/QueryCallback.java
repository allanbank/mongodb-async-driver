/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to convert a {@link Query} {@link Reply} into a
 * {@link MongoIteratorImpl}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class QueryCallback extends
        AbstractReplyCallback<MongoIterator<Document>> {

    /** The server the original request was sent to. */
    private volatile String myAddress;

    /** The original query. */
    private final Client myClient;

    /** The original query. */
    private final Query myQueryMessage;

    /** The server the original request was sent to. */
    private volatile Reply myReply;

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
            final Callback<MongoIterator<Document>> results) {

        super(results);

        myClient = client;
        myQueryMessage = queryMessage;
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
        // For races make sure that the iterator has the server name.
        if (myReply != null) {
            getForwardCallback().callback(convert(myReply));
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
                myQueryMessage, reply);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a {@link MongoIteratorImpl} around the reply.
     * </p>
     * 
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected MongoIterator<Document> convert(final Reply reply)
            throws MongoDbException {
        myReply = reply;
        if (myAddress != null) {
            return new MongoIteratorImpl(myQueryMessage, myClient, myAddress,
                    reply);
        }
        return null;
    }
}