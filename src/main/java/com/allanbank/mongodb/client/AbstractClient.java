/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
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
    public String send(final Callback<Reply> callback,
            final Message... messages) {
        return findConnection(messages).send(callback, messages);
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
    public String send(final Message... messages) {
        return findConnection(messages).send(null, messages);
    }

    /**
     * Locates a {@link Connection} to send a message on.
     * 
     * @param messages
     *            The messages that will be sent. The connection return should
     *            be compatible with all of the messages {@link ReadPreference}.
     * 
     * @return The {@link Connection} to send a message on.
     * @throws MongoDbException
     *             In the case of an error finding a {@link Connection}.
     */
    protected abstract Connection findConnection(Message[] messages)
            throws MongoDbException;

}