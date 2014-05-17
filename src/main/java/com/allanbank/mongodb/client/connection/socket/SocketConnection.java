/*
 * Copyright 2011-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.net.Socket;
import java.net.SocketException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.RandomAccessOutputStream;
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

    /**
     * The buffers used each connection. Each buffer is shared by all
     * connections but there can be up to 1 buffer per application thread.
     */
    private final ThreadLocal<Reference<BufferingBsonOutputStream>> myBuffers;

    /** The thread receiving replies. */
    private final Thread myReceiver;

    /** The sequence for serializing sends. */
    private final Sequence mySendSequence;

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
        this(server, config,
                new ThreadLocal<Reference<BufferingBsonOutputStream>>());
    }

    /**
     * Creates a new SocketConnection to a MongoDB server.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param buffers
     *            The buffers used each connection. Each buffer is shared by all
     *            connections but there can be up to 1 buffer per application
     *            thread.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public SocketConnection(final Server server,
            final MongoClientConfiguration config,
            final ThreadLocal<Reference<BufferingBsonOutputStream>> buffers)
            throws SocketException, IOException {
        super(server, config);

        myBuffers = buffers;

        mySendSequence = new Sequence(1L, config.getLockType());

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

            // Serialize the messages now so the critical section becomes close
            // to a write(byte[]) (with a little accounting overhead).
            final Reference<BufferingBsonOutputStream> outRef = myBuffers.get();
            BufferingBsonOutputStream out = (outRef != null) ? outRef.get()
                    : null;
            if (out == null) {
                out = new BufferingBsonOutputStream(
                        new RandomAccessOutputStream());
                out.setMaxCachedStringEntries(myConfig
                        .getMaxCachedStringEntries());
                out.setMaxCachedStringLength(myConfig
                        .getMaxCachedStringLength());
                myBuffers
                        .set(new SoftReference<BufferingBsonOutputStream>(out));
            }

            message1.write((int) (seq & 0xFFFFFF), out);
            if (message2 != null) {
                message2.write((int) ((seq + 1) & 0xFFFFFF), out);
            }

            // Now stand in line.
            mySendSequence.waitFor(seq);

            if (count == 1) {
                pendingMessage.set((int) (seq & 0xFFFFFF), message1,
                        replyCallback);
                send(pendingMessage, out.getOutput());
            }
            else {
                pendingMessage.set((int) ((seq + 1) & 0xFFFFFF), message2,
                        replyCallback);
                send(pendingMessage, out.getOutput());
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
