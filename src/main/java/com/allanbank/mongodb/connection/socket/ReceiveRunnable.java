/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import java.util.logging.Level;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Runnable to receive messages from an {@link AbstractSocketConnection}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class ReceiveRunnable implements Runnable {

    /** The {@link PendingMessage} used for the local cached copy. */
    private final PendingMessage myPendingMessage = new PendingMessage();

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
            while (mySocketConnection.myOpen.get()) {
                try {
                    receiveOne();

                    // Check if we are shutdown. Note the shutdown() method
                    // makes sure the last message gets a reply.
                    if (mySocketConnection.myShutdown.get()
                            && mySocketConnection.isIdle()) {
                        // All done.
                        return;
                    }
                }
                catch (final MongoDbException error) {
                    if (mySocketConnection.myOpen.get()) {
                        mySocketConnection.myLog.log(
                                Level.WARNING,
                                "Error reading a message: "
                                        + error.getMessage(), error);
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

    /**
     * Receives and process a single message.
     */
    protected final void receiveOne() {
        final Message received = mySocketConnection.doReceive();
        if (received instanceof Reply) {
            final Reply reply = (Reply) received;
            final int replyId = reply.getResponseToId();
            boolean took = false;

            // Keep polling the pending queue until we get to
            // message based on a matching replyId.
            try {
                took = mySocketConnection.myPendingQueue.poll(myPendingMessage);
                while (took && (myPendingMessage.getMessageId() != replyId)) {

                    // Note that this message will not get a reply.
                    mySocketConnection.raiseError(
                            AbstractSocketConnection.NO_REPLY,
                            myPendingMessage.getReplyCallback());

                    // Keep looking.
                    took = mySocketConnection.myPendingQueue
                            .poll(myPendingMessage);
                }

                if (took) {
                    // Must be the pending message's reply.
                    mySocketConnection.reply(reply,
                            myPendingMessage.getReplyCallback());
                }
                else {
                    mySocketConnection.myLog
                            .warning("Could not find the callback for reply '"
                                    + replyId + "'.");
                }
            }
            finally {
                myPendingMessage.clear();
            }
        }
        else if (received != null) {
            mySocketConnection.myLog.warning("Received a non-Reply message: "
                    + received);
        }
    }
}