/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.beans.PropertyChangeListener;
import java.io.Closeable;
import java.io.Flushable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Provides the lowest level interface for interacting with a MongoDB server.
 * The method provided here are a straight forward mapping from the <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol">MongoDB Wire
 * Protocol</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Connection extends Closeable, Flushable {

    /** The collection to use when issuing commands to the database. */
    public static final String COMMAND_COLLECTION = "$cmd";

    /** The property for if the connection is open or not. */
    public static final String OPEN_PROP_NAME = "open";

    /**
     * Adds the pending messages from the specified list.
     * 
     * @param pending
     *            The list to populate the pending list with.
     */
    public void addPending(List<PendingMessage> pending);

    /**
     * Adds a {@link PropertyChangeListener} to this connection. Events are
     * fired as the state of the connection changes.
     * 
     * @param listener
     *            The listener for the change events.
     */
    public void addPropertyChangeListener(PropertyChangeListener listener);

    /**
     * Removes any to be sent pending messages into the specified list.
     * 
     * @param pending
     *            The list to populate with the pending messages.
     */
    public void drainPending(List<PendingMessage> pending);

    /**
     * Returns the number of messages that are pending responses from the
     * server.
     * 
     * @return The number of messages pending responses from the server.
     */
    public int getPendingCount();

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
     * Notifies the call backs for the pending and optionally the to be sent
     * messages that there has been an external, unrecoverable error.
     * 
     * @param exception
     *            The error condition.
     * @param notifyToBeSent
     *            If true then the to be sent message's callback are also
     *            notified, otherwise just the pending messages are notified.
     */
    public void raiseErrors(MongoDbException exception, boolean notifyToBeSent);

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
     * @param reply
     *            The callback to notify of responses to the messages. May be
     *            <code>null</code>.
     * @param messages
     *            The messages to send on the connection. The messages will be
     *            sent one after the other and are guaranteed to be contiguous
     *            and have sequential message ids.
     * @return The server that was sent the request.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public String send(Callback<Reply> reply, Message... messages)
            throws MongoDbException;

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
