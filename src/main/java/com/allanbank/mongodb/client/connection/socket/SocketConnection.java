/*
 * Copyright 2011-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.AddressAware;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.message.BuildInfo;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides a blocking Socket based connection to a MongoDB server.
 * <p>
 * This version uses single receive thread. Sending of messages is done by the
 * application thread sending the message.
 * </p>
 * <p>
 * This implementation does not perform as well as the
 * {@link TwoThreadSocketConnection} when the {@link Socket} implementation does
 * not have built in buffering of messages or requires acknowledgment of
 * messages before releasing a write request. For that reason this
 * implementation is only used with the normal Java Socket implementations.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnection extends AbstractSocketConnection {

    /** The thread receiving replies. */
    final Thread myReceiver;

    /** The sequence for serializing sends. */
    final Sequence mySendSequence;

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
    public SocketConnection(final Server server,
            final MongoClientConfiguration config) throws SocketException,
            IOException {
        super(server, config);

        mySendSequence = new Sequence(1L);

        myReceiver = config.getThreadFactory().newThread(
                new ReceiveRunnable(this));
        myReceiver.setDaemon(true);
        myReceiver.setName("MongoDB " + mySocket.getLocalPort() + "<--"
                + myServer.getCanonicalName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (myOpen.compareAndSet(true, false)) {
            myReceiver.interrupt();

            // Now that output is shutdown. Close up the socket. This
            // Triggers the receiver to close if the interrupt didn't work.
            myOutput.close();
            myInput.close();
            mySocket.close();

            try {
                if (Thread.currentThread() != myReceiver) {
                    myReceiver.join();
                }
            }
            catch (final InterruptedException ie) {
                // Ignore.
            }

            myEventSupport.firePropertyChange(OPEN_PROP_NAME, true, false);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the send and pending queues are empty.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return super.isIdle() && mySendSequence.isIdle();
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

        final int count = (message2 == null) ? 1 : 2;
        final long seq = mySendSequence.reserve(count);
        final long end = seq + count;

        boolean sawError = false;
        final PendingMessage pendingMessage = new PendingMessage();
        try {
            mySendSequence.waitFor(seq);

            if (count == 1) {
                pendingMessage.set((int) (seq & 0xFFFFFF), message1,
                        replyCallback);
                send(pendingMessage);
            }
            else {
                pendingMessage.set((int) (seq & 0xFFFFFF), message1, null);
                send(pendingMessage);
                pendingMessage.set((int) ((seq + 1) & 0xFFFFFF), message2,
                        replyCallback);
                send(pendingMessage);
            }

            // If no-one is waiting we need to flush the message.
            if (mySendSequence.noWaiter(end)) {
                if (myReceiver != Thread.currentThread()) {
                    flush();
                }
                else {
                    markReaderNeedsToFlush();
                }
            }
        }
        catch (final InterruptedException ie) {
            // Handled by loop but if we have a message, need to
            // tell him something bad happened (but we shouldn't).
            raiseError(ie, pendingMessage.getReplyCallback());
        }
        catch (final IOException ioe) {
            myLog.warn(ioe, "I/O Error sending a message.");
            raiseError(ioe, pendingMessage.getReplyCallback());
            sawError = true;
        }
        catch (final RuntimeException re) {
            myLog.warn(re, "Runtime error sending a message.");
            raiseError(re, pendingMessage.getReplyCallback());
            sawError = true;
        }
        catch (final Error error) {
            myLog.error(error, "Error sending a message.");
            raiseError(error, pendingMessage.getReplyCallback());
            sawError = true;
        }
        finally {
            pendingMessage.clear();
            mySendSequence.release(seq, end);

            if (sawError) {
                // This may/will fail because we are dying.
                try {
                    if (myOpen.get()) {
                        flush();
                    }
                }
                catch (final IOException ioe) {
                    myLog.warn(ioe, "I/O Error on final flush of messages.");
                }
                finally {
                    // Make sure we get shutdown completely.
                    IOUtils.close(SocketConnection.this);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(final Message message, final ReplyCallback replyCallback)
            throws MongoDbException {
        send(message, null, replyCallback);
    }

    /**
     * Starts the connections read and write threads.
     */
    @Override
    public void start() {
        myReceiver.start();

        if (myServer.needBuildInfo()) {
            send(new BuildInfo(), new ServerUpdateCallback(myServer));
        }
    }
}
