/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;

/**
 * ConnectionLostException provides a exception thrown when the connection to
 * the MongoDB server is lost.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConnectionLostException extends MongoDbException {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -1905095594463181344L;

    /**
     * Creates a new ConnectionLostException.
     */
    public ConnectionLostException() {
        super();
    }

    /**
     * Creates a new ConnectionLostException.
     * 
     * @param message
     *            Reason for the exception.
     */
    public ConnectionLostException(final String message) {
        super(message);
    }

    /**
     * Creates a new ConnectionLostException.
     * 
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the {@link ConnectionLostException}.
     */
    public ConnectionLostException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new ConnectionLostException.
     * 
     * @param cause
     *            The exception causing the {@link ConnectionLostException}.
     */
    public ConnectionLostException(final Throwable cause) {
        super(cause);
    }

}
