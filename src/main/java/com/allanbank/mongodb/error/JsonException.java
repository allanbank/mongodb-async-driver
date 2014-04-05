/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;

/**
 * JsonException provides an exception to throw when processing JSON documents
 * fail.
 *
 * @api.yes This exception is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonException extends MongoDbException {

    /** Serialization version of the class. */
    private static final long serialVersionUID = 8248891467581639959L;

    /**
     * Creates a new JsonParseException.
     */
    public JsonException() {
        super();
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     */
    public JsonException(final String message) {
        super(message);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonException(final Throwable cause) {
        super(cause);
    }
}
