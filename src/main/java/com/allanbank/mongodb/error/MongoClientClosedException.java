/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Message;

/**
 * MongoClientClosedException is thrown when there is an attempt to send a
 * message on a closed {@link MongoClient}.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientClosedException extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 1729264905521755667L;

    /** The message that was being sent. */
    private final Message myMessage;

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
