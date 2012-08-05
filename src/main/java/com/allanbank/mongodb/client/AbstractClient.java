/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.net.SocketAddress;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
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
     * @see Client#send(Callback,Message[])
     */
    @Override
    public SocketAddress send(final Callback<Reply> callback,
            final Message... messages) {
        return findConnection().send(callback, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to locate the .
     * </p>
     * 
     * @see Client#send(Message[])
     */
    @Override
    public SocketAddress send(final Message... messages) {
        return findConnection().send(null, messages);
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