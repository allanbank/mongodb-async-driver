/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import java.util.logging.Level;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Runnable to receive messages from an {@link AbstractSocketConnection}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class ReceiveRunnable implements Runnable {

    /** The socket we are reading from. */
    private final AbstractSocketConnection mySocketConnection;

    /**
     * Creates a new ReceiveRunnable.
     * 
     * @param socketConnection
     *            The socket we are reading from.
     */
    /* package */ReceiveRunnable(final AbstractSocketConnection socketConnection) {
        mySocketConnection = socketConnection;
    }

    /**
     * Processing thread for receiving responses from the server.
     */
    @Override
    public void run() {
        try {
            while (mySocketConnection.isOpen()) {
                try {
                    mySocketConnection.doReceiveOne();

                    // Check if we are shutdown. Note the shutdown() method
                    // makes sure the last message gets a reply.
                    if (mySocketConnection.isShuttingDown()
                            && mySocketConnection.isIdle()) {
                        // All done.
                        return;
                    }
                }
                catch (final MongoDbException error) {
                    if (mySocketConnection.isOpen()) {
                        mySocketConnection.myLog.log(
                                Level.WARNING,
                                "Error reading a message: "
                                        + error.getMessage(), error);

                        mySocketConnection
                                .shutdown(new ConnectionLostException(error));
                    }
                    // All done.
                    return;
                }
            }
        }
        finally {
            // Make sure the connection is closed completely.
            IOUtils.close(mySocketConnection);
        }
    }
}