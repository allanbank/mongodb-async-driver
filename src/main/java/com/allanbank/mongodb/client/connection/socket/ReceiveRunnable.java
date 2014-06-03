/*
 * #%L
 * ReceiveRunnable.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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

                        mySocketConnection.shutdown(
                                new ConnectionLostException(error), false);
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