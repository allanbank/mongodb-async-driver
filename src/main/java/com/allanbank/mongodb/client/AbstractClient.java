/*
 * #%L
 * AbstractClient.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.client;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.error.MongoClientClosedException;

/**
 * AbstractClient provides a base class for {@link Client} implementations.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractClient implements Client {

    /** Tracks if the client is closed. */
    private final AtomicBoolean myClosed = new AtomicBoolean(false);

    /**
     * Creates a new AbstractClient.
     */
    public AbstractClient() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close all of the open connections.
     * </p>
     * 
     * @see Closeable#close()
     */
    @Override
    public void close() {
        myClosed.set(true);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to locate the a connection to send the messages and then
     * forward the messages to that connection.
     * </p>
     */
    @Override
    public void send(final Message message1, final Message message2,
            final ReplyCallback replyCallback) throws MongoDbException {

        assertOpen(message1);

        findConnection(message1, message2).send(message1, message2,
                replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to locate the a connection to send the message and then
     * forward the message to that connection.
     * </p>
     */
    @Override
    public void send(final Message message, final ReplyCallback replyCallback)
            throws MongoDbException {

        assertOpen(message);

        findConnection(message, null).send(message, replyCallback);
    }

    /**
     * Locates a {@link Connection} to send a message on.
     * 
     * @param message1
     *            The first message that will be sent. The connection return
     *            should be compatible with all of the messages
     *            {@link ReadPreference}.
     * @param message2
     *            The second message that will be sent. The connection return
     *            should be compatible with all of the messages
     *            {@link ReadPreference}. May be <code>null</code>.
     * 
     * @return The {@link Connection} to send a message on.
     * @throws MongoDbException
     *             In the case of an error finding a {@link Connection}.
     */
    protected abstract Connection findConnection(Message message1,
            Message message2) throws MongoDbException;

    /**
     * Asserts that the command is open.
     * 
     * @param message
     *            The message being sent.
     * @throws MongoClientClosedException
     *             If the client has been closed.
     */
    private void assertOpen(final Message message)
            throws MongoClientClosedException {
        if (myClosed.get()) {
            throw new MongoClientClosedException(message);
        }
    }

}