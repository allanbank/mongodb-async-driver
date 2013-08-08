/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.beans.PropertyChangeListener;
import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Provides the lowest level interface for interacting with a MongoDB server.
 * The method provided here are a straight forward mapping from the <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol">MongoDB Wire
 * Protocol</a>.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Connection extends Closeable, Flushable {

    /** The collection to use when issuing commands to the database. */
    public static final String COMMAND_COLLECTION = "$cmd";

    /** The property for if the connection is open or not. */
    public static final String OPEN_PROP_NAME = "open";

    /**
     * Adds a {@link PropertyChangeListener} to this connection. Events are
     * fired as the state of the connection changes.
     * 
     * @param listener
     *            The listener for the change events.
     */
    public void addPropertyChangeListener(PropertyChangeListener listener);

    /**
     * Returns the number of messages that are pending responses from the
     * server.
     * 
     * @return The number of messages pending responses from the server.
     */
    public int getPendingCount();

    /**
     * Returns the name of a server the connection is currently connected to.
     * 
     * @return The name of a server the connection is currently connected to.
     */
    public String getServerName();

    /**
     * Determines if the connection is idle.
     * 
     * @return True if the connection is waiting for messages to send and has no
     *         outstanding messages to receive.
     */
    public boolean isIdle();

    /**
     * Determines if the connection is open.
     * 
     * @return True if the connection is open and thinks it can send messages.
     */
    public boolean isOpen();

    /**
     * Notifies the call backs for the pending messages that there has been an
     * external, unrecoverable error.
     * 
     * @param exception
     *            The error condition.
     */
    public void raiseErrors(MongoDbException exception);

    /**
     * Removes a {@link PropertyChangeListener} from this connection.
     * 
     * @param listener
     *            The listener for the change events.
     */
    public void removePropertyChangeListener(PropertyChangeListener listener);

    /**
     * Sends a message on the connection.
     * 
     * @param message
     *            The message to send on the connection.
     * @param replyCallback
     *            The callback to notify of responses to the messages. May be
     *            <code>null</code>.
     * @return The server that was sent the request.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public String send(Message message, Callback<Reply> replyCallback)
            throws MongoDbException;

    /**
     * Sends a message on the connection.
     * 
     * @param message1
     *            The first message to send on the connection.
     * @param message2
     *            The second message to send on the connection.
     * @param replyCallback
     *            The callback to notify of responses to the {@code message2}.
     *            May be <code>null</code>.
     * @return The server that was sent the request.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public String send(Message message1, Message message2,
            Callback<Reply> replyCallback) throws MongoDbException;

    /**
     * Notifies the connection that once all outstanding requests have been sent
     * and all replies received the Connection should be closed. This method
     * will return prior to the connection being closed.
     */
    public void shutdown();

    /**
     * Waits for the connection to become idle.
     * 
     * @param timeout
     *            The amount of time to wait for the connection to become idle.
     * @param timeoutUnits
     *            The units for the amount of time to wait for the connection to
     *            become idle.
     */
    public void waitForClosed(int timeout, TimeUnit timeoutUnits);
}
