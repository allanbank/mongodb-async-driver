/*
 * #%L
 * MongoClientClosedException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.error;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;

/**
 * MongoClientClosedException is thrown when there is an attempt to send a
 * message on a closed {@link MongoClient}.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class MongoClientClosedException
        extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 1729264905521755667L;

    /** The message that was being sent. */
    private transient final Message myMessage;

    /**
     * Creates a new CannotConnectException.
     */
    public MongoClientClosedException() {
        super();
        myMessage = null;
    }

    /**
     * Creates a new CannotConnectException.
     *
     * @param message
     *            The message that was being sent.
     */
    public MongoClientClosedException(final Message message) {
        super("MongoClient has been closed.");
        myMessage = message;
    }

    /**
     * Creates a new CannotConnectException.
     *
     * @param message
     *            Message for the exception.
     */
    public MongoClientClosedException(final String message) {
        super(message);
        myMessage = null;
    }

    /**
     * Creates a new CannotConnectException.
     *
     * @param message
     *            Message for the exception.
     * @param cause
     *            The cause of the error.
     */
    public MongoClientClosedException(final String message,
            final Throwable cause) {
        super(message, cause);
        myMessage = null;
    }

    /**
     * Creates a new CannotConnectException.
     *
     * @param cause
     *            The cause of the error.
     */
    public MongoClientClosedException(final Throwable cause) {
        super(cause);
        myMessage = null;
    }

    /**
     * Returns the message that was being sent.
     *
     * @return The message that was being sent.
     */
    public Message getSentMessage() {
        return myMessage;
    }
}
