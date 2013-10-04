/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.concurrent.atomic.AtomicBoolean;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.MongoIteratorImpl;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to convert a {@link Query} {@link Reply} into a
 * {@link MongoIteratorImpl}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class QueryCallback extends
        AbstractReplyCallback<MongoIterator<Document>> {

    /** The server the original request was sent to. */
    private volatile String myAddress;

    /** The original query. */
    private final Client myClient;

    /** The original query. */
    private final Query myQueryMessage;

    /** The reply to the query. */
    private volatile Reply myReply;

    /**
     * Initially set to false. Set to true for the first of address or reply
     * being set. The second fails and {@link #trigger() triggers} the callback.
     */
    private final AtomicBoolean mySetOther;

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

        mySetOther = new AtomicBoolean(false);
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
        trigger();
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
        return new MongoIteratorImpl(myQueryMessage, myClient, myAddress, reply);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to check if the server address has been set and if so then
     * pass the converted reply to the {@link #getForwardCallback() forward
     * callback}. Otherwise the call is dropped.
     * </p>
     */
    @Override
    protected void handle(final Reply reply) {
        myReply = reply;
        trigger();
    }

    /**
     * Triggers the callback when the address and reply are set.
     */
    private void trigger() {
        if (!mySetOther.compareAndSet(false, true)) {
            super.handle(myReply);
        }
    }
}