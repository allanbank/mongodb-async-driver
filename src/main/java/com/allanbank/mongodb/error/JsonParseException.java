/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

/**
 * JsonParseException provides an exception to throw when parsing a JSON
 * document fails.
 *
 * @api.yes This exception is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonParseException extends JsonException {

    /** Serialization version of the class. */
    private static final long serialVersionUID = 8248891467581639959L;

    /** The approximate column where the parse failed. */
    private final int myColumn;

    /** The approximate line where the parse failed. */
    private final int myLine;

    /**
     * Creates a new JsonParseException.
     */
    public JsonParseException() {
        super();
        myLine = -1;
        myColumn = -1;
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     */
    public JsonParseException(final String message) {
        super(message);
        myLine = -1;
        myColumn = -1;
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     * @param line
     *            The approximate line where the parse failed.
     * @param column
     *            The approximate column where the parse failed.
     */
    public JsonParseException(final String message, final int line,
            final int column) {
        super(message);
        myLine = line;
        myColumn = column;
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonParseException(final String message, final Throwable cause) {
        super(message, cause);
        myLine = -1;
        myColumn = -1;
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message
     *            Reason for the exception.
     * @param cause
     *            The exception causing the MongoDbException.
     * @param line
     *            The approximate line where the parse failed.
     * @param column
     *            The approximate column where the parse failed.
     */
    public JsonParseException(final String message, final Throwable cause,
            final int line, final int column) {
        super(message, cause);
        myLine = line;
        myColumn = column;
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param cause
     *            The exception causing the MongoDbException.
     */
    public JsonParseException(final Throwable cause) {
        super(cause);
        myLine = -1;
        myColumn = -1;
    }

    /**
     * Returns the approximate column where the parse failed. Returns a negative
     * value if the column is not known.
     *
     * @return The approximate column where the parse failed.
     */
    public int getColumn() {
        return myColumn;
    }

    /**
     * Returns the approximate line where the parse failed. Returns a negative
     * value if the line is not known.
     *
     * @return The approximate line where the parse failed.
     */
    public int getLine() {
        return myLine;
    }
}
