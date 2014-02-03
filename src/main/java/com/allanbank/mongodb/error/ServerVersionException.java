/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import java.io.IOException;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;

/**
 * ServerVersionException is thrown to report that an attempt was made to send a
 * request to a server that required a more recent version of the server.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerVersionException extends MongoDbException {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -8577756570001826274L;

    /**
     * Creates a suitable message for the exception.
     * 
     * @param operation
     *            The name of the command/operation.
     * @param range
     *            The required server version range for the operation.
     * @param actual
     *            The actual version from the server.
     * @return The message for the exception.
     */
    private static String message(final String operation,
            final VersionRange range, final Version actual) {
        if (Version.VERSION_0.equals(range.getLowerBounds())) {
            return "Attempted to send the '" + operation
                    + "' operation to a version " + actual
                    + " server but the operation is only supported "
                    + "before version " + range.getUpperBounds() + ".";
        }
        else if (Version.UNKNOWN.equals(range.getUpperBounds())) {
            return "Attempted to send the '" + operation
                    + "' operation to a version " + actual
                    + " server but the operation is only supported "
                    + "after version " + range.getLowerBounds() + ".";
        }
        else {
            return "Attempted to send the '" + operation
                    + "' operation to a version " + actual
                    + " server but the operation is only supported "
                    + "from version " + range.getLowerBounds() + " to "
                    + range.getUpperBounds() + ".";
        }
    }

    /** The actual server version. */
    private final Version myActualVersion;

    /** The operation's message. */
    private transient Message myMessage;

    /** The name of the operation. */
    private final String myOperation;

    /** The required server version for the operation. */
    private final VersionRange myRange;

    /**
     * Creates a new ServerVersionException.
     * 
     * @param operation
     *            The name of the command/operation.
     * @param range
     *            The required server version range for the operation.
     * @param actual
     *            The actual version from the server.
     * @param message
     *            The operation's message.
     */
    public ServerVersionException(final String operation,
            final VersionRange range, final Version actual,
            final Message message) {
        super(message(operation, range, actual));

        myOperation = operation;
        myActualVersion = actual;
        myRange = range;
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
     * Returns the maximum (exclusive) server version for the operation.
     * 
     * @return The required server version for the operation.
     */
    public Version getMaximumVersion() {
        return myRange.getUpperBounds();
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
     * Returns the required (inclusive) server version for the operation.
     * 
     * @return The required server version for the operation.
     */
    public Version getRequiredVersion() {
        return myRange.getLowerBounds();
    }

    /**
     * Returns the operation's message.
     * 
     * @return The operation's message.
     */
    public Message getSentMessage() {
        return myMessage;
    }

    /**
     * Reads the serialized configuration and sets the transient field to known
     * values.
     * 
     * @param stream
     *            The stream to read from.
     * @throws IOException
     *             On a failure reading from the stream.
     * @throws ClassNotFoundException
     *             On a failure locating a type in the stream.
     */
    private void readObject(final java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        myMessage = null;
    }
}
