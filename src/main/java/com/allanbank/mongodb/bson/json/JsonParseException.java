/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.json;

import com.allanbank.mongodb.MongoDbException;

/**
 * JsonParseException provides an exception to throw when parsing a JSON
 * document fails.
 * 
 * @api.yes This exception is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonParseException extends MongoDbException {

    /** Serialization version of the class. */
    private static final long serialVersionUID = 8248891467581639959L;

    /**
     * Creates a new JsonParseException.
     */
    public JsonParseException() {
        super();
    }

    /**
     * Creates a new JsonParseException.
     * 
     * @param message
     */
    public JsonParseException(final String message) {
        super(message);
    }

    /**
     * Creates a new JsonParseException.
     * 
     * @param message
     * @param cause
     */
    public JsonParseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new JsonParseException.
     * 
     * @param cause
     */
    public JsonParseException(final Throwable cause) {
        super(cause);
    }
}
