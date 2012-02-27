/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;

/**
 * CannotConnectException is thrown to report a failure when attempting to
 * connect to MongoDB.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CannotConnectException extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 1729264905521755667L;

    /**
     * Creates a new CannotConnectException.
     */
    public CannotConnectException() {
        super();
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param message
     *            Message for the exception.
     */
    public CannotConnectException(final String message) {
        super(message);
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param message
     *            Message for the exception.
     * @param cause
     *            The cause of the error.
     */
    public CannotConnectException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new CannotConnectException.
     * 
     * @param cause
     *            The cause of the error.
     */
    public CannotConnectException(final Throwable cause) {
        super(cause);
    }
}
