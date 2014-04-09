/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;

/**
 * MongoDbAuthenticationException is thrown to report a failure to authenticate
 * with a MongoDB database.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbAuthenticationException extends MongoDbException {

    /** Serialization exception for the class. */
    private static final long serialVersionUID = 1729264905521755667L;

    /**
     * Creates a new MongoDbAuthenticationException.
     */
    public MongoDbAuthenticationException() {
        super();
    }

    /**
     * Creates a new MongoDbAuthenticationException.
     * 
     * @param message
     *            Message for the exception.
     */
    public MongoDbAuthenticationException(final String message) {
        super(message);
    }

    /**
     * Creates a new MongoDbAuthenticationException.
     * 
     * @param message
     *            Message for the exception.
     * @param cause
     *            The cause of the error.
     */
    public MongoDbAuthenticationException(final String message,
            final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new MongoDbAuthenticationException.
     * 
     * @param cause
     *            The cause of the error.
     */
    public MongoDbAuthenticationException(final Throwable cause) {
        super(cause);
    }
}
