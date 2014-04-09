/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

/**
 * Exception base class for all MongoDB exceptions.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbException extends RuntimeException {

    /** Serialization version for the class. */
    private static final long serialVersionUID = 8065038814148830471L;

    /**
     * Creates a new MongoDbException.
     */
    public MongoDbException() {
        super();
    }

    /**
     * Creates a new MongoDbException.
     * 
     * @param message
     *            Reason for the exception.
     */
    public MongoDbException(final String message) {
        super(message);
    }

    /**
     * Creates a new MongoDbException.
     * 
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public MongoDbException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new MongoDbException.
     * 
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public MongoDbException(final Throwable cause) {
        super((cause == null) ? "" : cause.getMessage(), cause);
    }

}
