/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * AbstractClient provides a base class for {@link Client} implementations.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractClient implements Client {

    /**
     * Creates a new AbstractClient.
     */
    public AbstractClient() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to locate the .
     * </p>
     * 
     * @see Client#send(GetMore,Callback)
     */
    @Override
    public void send(final GetMore getMore, final Callback<Reply> callback) {
        findConnection().send(callback, getMore);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} to MongoDB.
     * </p>
     * 
     * @see Client#send(Message)
     */
    @Override
    public void send(final Message message) {
        findConnection().send(message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} and {@link GetLastError} to
     * MongoDB.
     * </p>
     * 
     * @see Client#send(Message, GetLastError, Callback)
     */
    @Override
    public void send(final Message message, final GetLastError lastError,
            final Callback<Reply> callback) {
        findConnection().send(callback, message, lastError);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Query} to MongoDB.
     * </p>
     * 
     * @see Client#send(Query, Callback)
     */
    @Override
    public void send(final Query query, final Callback<Reply> callback) {
        findConnection().send(callback, query);
    }

    /**
     * Locates a {@link Connection} to send a message on.
     * 
     * @return The {@link Connection} to send a message on.
     * @throws MongoDbException
     *             In the case of an error finding a {@link Connection}.
     */
    protected abstract Connection findConnection() throws MongoDbException;

}