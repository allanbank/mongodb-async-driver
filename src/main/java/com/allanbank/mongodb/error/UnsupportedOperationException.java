/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.Message;

/**
 * UnsupportedOperationException is thrown to report that an attempt was made to
 * send a request to a server that required a more recent version of the server.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnsupportedOperationException extends MongoDbException {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -8577756570001826274L;

    /** The actual server version. */
    private final Version myActualVersion;

    /** The operation's message. */
    private final Message myMessage;

    /** The name of the operation. */
    private final String myOperation;

    /** The required server version for the operation. */
    private final Version myRequiredVersion;

    /**
     * Creates a new DocumentToLargeException.
     * 
     * @param operation
     *            The name of the command/operation.
     * @param required
     *            The required server version for the operation.
     * @param actual
     *            The actual version from the server.
     * @param message
     *            The operation's message.
     */
    public UnsupportedOperationException(final String operation,
            final Version required, final Version actual, final Message message) {
        super("Attempted to send the '" + operation + "' operation to a "
                + actual + " server but the operation is "
                + "only supported after " + required + ".");

        myOperation = operation;
        myActualVersion = actual;
        myRequiredVersion = required;
        myMessage = message;
    }

    /**
     * Returns the actual server version.
     * 
     * @return The actual server version.
     */
    public Version getActualVersion() {
        return myActualVersion;
    }

    /**
     * Returns the name of the operation.
     * 
     * @return The name of the operation.
     */
    public String getOperation() {
        return myOperation;
    }

    /**
     * Returns the operation's message.
     * 
     * @return The operation's message.
     */
    public Message getOperationsMessage() {
        return myMessage;
    }

    /**
     * Returns the required server version for the operation.
     * 
     * @return The required server version for the operation.
     */
    public Version getRequiredVersion() {
        return myRequiredVersion;
    }
}
