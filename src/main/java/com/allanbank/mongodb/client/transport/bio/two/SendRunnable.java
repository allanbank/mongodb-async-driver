/*
 * #%L
 * SendRunnable.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio.two;

import java.io.IOException;

import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyHandler;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Runnable to push data out over the MongoDB connection.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class SendRunnable
        implements Runnable {

    /** The logger for the send thread. */
    private static final Log LOG = LogFactory.getLog(SendRunnable.class);

    /** Tracks if there are messages in the buffer that need to be flushed. */
    private boolean myNeedToFlush = false;

    /** The output stream. */
    private final BufferingBsonOutputStream myOutput;

    /** The queue of messages to be sent. */
    private final PendingMessageQueue myToSendQueue;

    /** The transport. */
    private final TwoThreadTransport myTransport;

    /** The {@link PendingMessage} used for the local cached copy. */
    private final PendingMessage myWorkingMessage = new PendingMessage();

    /**
     * Creates a new SendRunnable.
     *
     * @param transport
     *            The transport.
     */
    /* package */SendRunnable(final TwoThreadTransport transport) {
        myTransport = transport;
        myOutput = transport.getBsonOut();
        myToSendQueue = transport.getToSendQueue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to pull messages off the {@link #myToSendQueue} and push them
     * into the socket connection. If <code>null</code> is ever received from a
     * poll of the queue then the socket connection is flushed and blocking call
     * is made to the queue.
     * </p>
     *
     * @see Runnable#run()
     */
    @Override
    public void run() {
        boolean sawError = false;
        try {
            while (myTransport.isOpen() && !sawError) {
                try {
                    sendOne();
                }
                catch (final InterruptedException ie) {
                    // Handled by loop but if we have a message, need to
                    // tell him something bad happened (but we shouldn't).
                    raiseError(ie, myWorkingMessage.getReplyCallback());
                }
                catch (final IOException ioe) {
                    LOG.warn(ioe, "I/O Error sending a message.");
                    raiseError(ioe, myWorkingMessage.getReplyCallback());
                    sawError = true;
                }
                catch (final RuntimeException re) {
                    LOG.warn(re, "Runtime error sending a message.");
                    raiseError(re, myWorkingMessage.getReplyCallback());
                    sawError = true;
                }
                catch (final Error error) {
                    LOG.error(error, "Error sending a message.");
                    raiseError(error, myWorkingMessage.getReplyCallback());
                    sawError = true;
                }
                finally {
                    myWorkingMessage.clear();
                }
            }
        }
        finally {
            // This may/will fail because we are dying.
            try {
                if (myTransport.isOpen()) {
                    doFlush();
                }
            }
            catch (final IOException ioe) {
                LOG.warn(ioe, "I/O Error on final flush of messages.");
            }
            finally {
                // Make sure we get shutdown completely.
                IOUtils.close(myTransport);
            }
        }
    }

    /**
     * Flushes the messages in the buffer and clears the need-to-flush flag.
     *
     * @throws IOException
     *             On a failure flushing the messages.
     */
    protected final void doFlush() throws IOException {
        if (myNeedToFlush) {
            flush();
            myNeedToFlush = false;
        }
    }

    /**
     * Updates to raise an error on the callback, if any.
     *
     * @param exception
     *            The thrown exception.
     * @param replyCallback
     *            The callback for the reply to the message.
     */
    protected void raiseError(final Throwable exception,
            final ReplyCallback replyCallback) {
        ReplyHandler.raiseError(exception, replyCallback, null);
    }

    /**
     * Sends a single message.
     *
     * @throws InterruptedException
     *             If the thread is interrupted waiting for a message to send.
     * @throws IOException
     *             On a failure sending the message.
     */
    protected final void sendOne() throws InterruptedException, IOException {
        boolean took = false;
        if (myNeedToFlush) {
            took = myToSendQueue.poll(myWorkingMessage);
        }
        else {
            myToSendQueue.take(myWorkingMessage);
            took = true;
        }

        if (took) {
            final int messageId = myWorkingMessage.getMessageId();
            final Message message = myWorkingMessage.getMessage();

            myNeedToFlush = true;
            message.write(messageId, myOutput);

            // We have handed the message off. Not our problem any more.
            // We could legitimately do this before the send but in the case
            // of an I/O error the send's exception is more meaningful then
            // the receivers generic "Didn't get a reply".
            myWorkingMessage.clear();
        }
        else {
            doFlush();
        }
    }

    /**
     * Flushes the output stream.
     *
     * @throws IOException
     *             On a failure flushing the stream.
     */
    private void flush() throws IOException {
        myOutput.flush();
    }
}