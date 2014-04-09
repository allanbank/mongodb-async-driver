/*
 * Copyright 2011-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;
import java.net.SocketException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.AddressAware;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.message.BuildInfo;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides a blocking Socket based connection to a MongoDB server.
 * <p>
 * This version uses a pair of threads (1 send and 1 receive) to handle the
 * messages going to and from MongoDB.
 * </p>
 * <p>
 * This implementation was the default for the driver through the 1.2.3 release.
 * It is still used by the driver for communication sockets that are not know to
 * be standard Java Sockets as it performs better when the communication path
 * does not have built in buffering of messages.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TwoThreadSocketConnection extends AbstractSocketConnection {

    /** The queue of messages to be sent. */
    protected final PendingMessageQueue myToSendQueue;

    /** The thread receiving replies. */
    private final Thread myReceiver;

    /** The thread sending messages. */
    private final Thread mySender;

    /**
     * Creates a new SocketConnection to a MongoDB server.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public TwoThreadSocketConnection(final Server server,
            final MongoClientConfiguration config) throws SocketException,
            IOException {
        super(server, config);

        myToSendQueue = new PendingMessageQueue(
                config.getMaxPendingOperationsPerConnection(),
                config.getLockType());

        myReceiver = config.getThreadFactory().newThread(
                new ReceiveRunnable(this));
        myReceiver.setDaemon(true);
        myReceiver.setName("MongoDB " + mySocket.getLocalPort() + "<--"
                + myServer.getCanonicalName());

        mySender = config.getThreadFactory().newThread(new SendRunnable());
        mySender.setDaemon(true);
        mySender.setName("MongoDB " + mySocket.getLocalPort() + "-->"
                + myServer.getCanonicalName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        final boolean wasOpen = myOpen.get();
        myOpen.set(false);

        mySender.interrupt();
        myReceiver.interrupt();

        try {
            if (Thread.currentThread() != mySender) {
                mySender.join();
            }
        }
        catch (final InterruptedException ie) {
            // Ignore.
        }
        finally {
            // Now that output is shutdown. Close up the socket. This
            // Triggers the receiver to close if the interrupt didn't work.
            myOutput.close();
            myInput.close();
            mySocket.close();
        }

        try {
            if (Thread.currentThread() != myReceiver) {
                myReceiver.join();
            }
        }
        catch (final InterruptedException ie) {
            // Ignore.
        }

        myEventSupport.firePropertyChange(OPEN_PROP_NAME, wasOpen, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPendingCount() {
        return super.getPendingCount() + myToSendQueue.size();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the send and pending queues are empty.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return super.isIdle() && myToSendQueue.isEmpty();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Notifies the appropriate messages of the error.
     * </p>
     */
    @Override
    public void raiseErrors(final MongoDbException exception) {
        final PendingMessage message = new PendingMessage();
        while (myToSendQueue.poll(message)) {
            raiseError(exception, message.getReplyCallback());
        }

        super.raiseErrors(exception);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(final Message message1, final Message message2,
            final ReplyCallback replyCallback) throws MongoDbException {

        validate(message1, message2);

        if (replyCallback instanceof AddressAware) {
            ((AddressAware) replyCallback).setAddress(myServer
                    .getCanonicalName());
        }

        try {
            myToSendQueue.put(message1, null, message2, replyCallback);
        }
        catch (final InterruptedException e) {
            throw new MongoDbException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(final Message message, final ReplyCallback replyCallback)
            throws MongoDbException {

        validate(message, null);

        if (replyCallback instanceof AddressAware) {
            ((AddressAware) replyCallback).setAddress(myServer
                    .getCanonicalName());
        }

        try {
            myToSendQueue.put(message, replyCallback);
        }
        catch (final InterruptedException e) {
            throw new MongoDbException(e);
        }
    }

    /**
     * Starts the connections read and write threads.
     */
    @Override
    public void start() {
        myReceiver.start();
        mySender.start();

        if (myServer.needBuildInfo()) {
            send(new BuildInfo(), new ServerUpdateCallback(myServer));
        }
    }

    /**
     * Runnable to push data out over the MongoDB connection.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class SendRunnable implements Runnable {

        /** Tracks if there are messages in the buffer that need to be flushed. */
        private boolean myNeedToFlush = false;

        /** The {@link PendingMessage} used for the local cached copy. */
        private final PendingMessage myPendingMessage = new PendingMessage();

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to pull messages off the
         * {@link TwoThreadSocketConnection#myToSendQueue} and push them into
         * the socket connection. If <code>null</code> is ever received from a
         * poll of the queue then the socket connection is flushed and blocking
         * call is made to the queue.
         * </p>
         * 
         * @see Runnable#run()
         */
        @Override
        public void run() {
            boolean sawError = false;
            try {
                while (myOpen.get() && !sawError) {
                    try {
                        sendOne();
                    }
                    catch (final InterruptedException ie) {
                        // Handled by loop but if we have a message, need to
                        // tell him something bad happened (but we shouldn't).
                        raiseError(ie, myPendingMessage.getReplyCallback());
                    }
                    catch (final IOException ioe) {
                        myLog.warn(ioe, "I/O Error sending a message.");
                        raiseError(ioe, myPendingMessage.getReplyCallback());
                        sawError = true;
                    }
                    catch (final RuntimeException re) {
                        myLog.warn(re, "Runtime error sending a message.");
                        raiseError(re, myPendingMessage.getReplyCallback());
                        sawError = true;
                    }
                    catch (final Error error) {
                        myLog.error(error, "Error sending a message.");
                        raiseError(error, myPendingMessage.getReplyCallback());
                        sawError = true;
                    }
                    finally {
                        myPendingMessage.clear();
                    }
                }
            }
            finally {
                // This may/will fail because we are dying.
                try {
                    if (myOpen.get()) {
                        doFlush();
                    }
                }
                catch (final IOException ioe) {
                    myLog.warn(ioe, "I/O Error on final flush of messages.");
                }
                finally {
                    // Make sure we get shutdown completely.
                    IOUtils.close(TwoThreadSocketConnection.this);
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
         * Sends a single message.
         * 
         * @throws InterruptedException
         *             If the thread is interrupted waiting for a message to
         *             send.
         * @throws IOException
         *             On a failure sending the message.
         */
        protected final void sendOne() throws InterruptedException, IOException {
            boolean took = false;
            if (myNeedToFlush) {
                took = myToSendQueue.poll(myPendingMessage);
            }
            else {
                myToSendQueue.take(myPendingMessage);
                took = true;
            }

            if (took) {
                myNeedToFlush = true;
                send(myPendingMessage);

                // We have handed the message off. Not our problem any more.
                // We could legitimately do this before the send but in the case
                // of an I/O error the send's exception is more meaningful then
                // the receivers generic "Didn't get a reply".
                myPendingMessage.clear();
            }
            else {
                doFlush();
            }
        }
    }
}
