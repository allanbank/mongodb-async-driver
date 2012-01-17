/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.messsage.Delete;
import com.allanbank.mongodb.connection.messsage.GetMore;
import com.allanbank.mongodb.connection.messsage.Header;
import com.allanbank.mongodb.connection.messsage.Insert;
import com.allanbank.mongodb.connection.messsage.KillCursors;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.messsage.Update;

/**
 * Provides a blocking Socket based connection to a MongoDB server.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnection implements Connection {

    /** The length of the message header in bytes. */
    public static final int HEADER_LENGTH = 16;

    /** The logger for the {@link SocketConnection}. */
    protected static final Logger LOG = Logger.getLogger(SocketConnection.class
            .getCanonicalName());

    /** Holds if the connection is open. */
    protected final AtomicBoolean myOpen;

    /** The queue of messages sent but waiting for a reply. */
    protected final BlockingQueue<PendingMessage> myPendingQueue;

    /** The queue of messages to be sent. */
    protected final BlockingQueue<PendingMessage> myToSendQueue;

    /** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
    private final BsonInputStream myBsonIn;

    /** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
    private final BsonOutputStream myBsonOut;

    /** The buffered input stream. */
    private final BufferedInputStream myInput;

    /** The next message id. */
    private int myNextId;

    /** The buffered output stream. */
    private final BufferedOutputStream myOutput;

    /** The thread receiving replies. */
    private final Thread myReceiver;

    /** The thread sending messages. */
    private final Thread mySender;

    /** The open socket. */
    private final Socket mySocket;

    /**
     * Creates a new SocketConnection to a MongoDB server.
     * 
     * @param address
     *            The address of the MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public SocketConnection(final InetSocketAddress address,
            final MongoDbConfiguration config) throws SocketException,
            IOException {

        myOpen = new AtomicBoolean(false);

        mySocket = new Socket();

        mySocket.setKeepAlive(config.isUsingSoKeepalive());
        mySocket.setSoTimeout(config.getReadTimeout());
        mySocket.connect(address, config.getConnectTimeout());

        myOpen.set(true);

        myInput = new BufferedInputStream(mySocket.getInputStream());
        myBsonIn = new BsonInputStream(myInput);

        myOutput = new BufferedOutputStream(mySocket.getOutputStream());
        myBsonOut = new BsonOutputStream(myOutput);

        myNextId = 1;

        myToSendQueue = new ArrayBlockingQueue<PendingMessage>(
                config.getMaxPendingOperationsPerConnection());
        myPendingQueue = new ArrayBlockingQueue<PendingMessage>(
                config.getMaxPendingOperationsPerConnection());

        myReceiver = config.getThreadFactory().newThread(new ReceiveRunnable());
        myReceiver.setName("MongoDB Receiver " + mySocket.getLocalPort()
                + "-->" + address.toString());
        myReceiver.start();

        mySender = config.getThreadFactory().newThread(new SendRunnable());
        mySender.setName("MongoDB Sender " + mySocket.getLocalPort() + "-->"
                + address.toString());
        mySender.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        myOpen.set(false);

        mySender.interrupt();
        myReceiver.interrupt();

        try {
            mySender.join();
        }
        catch (final InterruptedException ie) {
            // Ignore.
        }
        finally {
            myOutput.close();
            myInput.close();
            mySocket.close();
        }

        try {
            myReceiver.join();
        }
        catch (final InterruptedException ie) {
            // Ignore.
        }
    }

    /**
     * /** {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        myOutput.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPendingMessageCount() {
        return myPendingQueue.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getToBeSentMessageCount() {
        return myToSendQueue.size();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the send and pending queues are empty.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return myPendingQueue.isEmpty() && myToSendQueue.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void send(final Callback<Reply> reply,
            final Message... messages) throws MongoDbException {
        try {
            for (final Message message : messages) {
                myToSendQueue.put(new PendingMessage(nextId(), message, reply));
            }
        }
        catch (final InterruptedException e) {
            throw new MongoDbException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void send(final Message... messages)
            throws MongoDbException {
        try {
            for (final Message message : messages) {
                myToSendQueue.put(new PendingMessage(nextId(), message));
            }
        }
        catch (final InterruptedException e) {
            throw new MongoDbException(e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link Connection} returned from
     * {@link #ensureConnected()}.
     * </p>
     */
    @Override
    public void waitForIdle(final int timeout, final TimeUnit timeoutUnits) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeoutUnits.toMillis(timeout);

        while (!isIdle() && (now < deadline)) {
            try {
                // A slow spin loop.
                TimeUnit.MILLISECONDS.sleep(10);
            }
            catch (final InterruptedException e) {
                // Ignore.
                e.hashCode();
            }
            now = System.currentTimeMillis();
        }
    }

    /**
     * Receives a single message from the connection.
     * 
     * @return The {@link Message} received.
     * @throws MongoDbException
     *             On an error receiving the message.
     */
    protected Message doReceive() throws MongoDbException {
        try {
            final int length = myBsonIn.readInt();
            final int requestId = myBsonIn.readInt();
            final int responseId = myBsonIn.readInt();
            final int opCode = myBsonIn.readInt();

            final Operation op = Operation.fromCode(opCode);
            if (op == null) {
                // Huh? Dazed and confused
                throw new MongoDbException("Unexpected operation read '"
                        + opCode + "'.");
            }

            final Header header = new Header(length, requestId, responseId, op);
            Message message = null;
            switch (op) {
            case REPLY:
                message = new Reply(header, myBsonIn);
                break;
            case QUERY:
                message = new Query(header, myBsonIn);
                break;
            case UPDATE:
                message = new Update(myBsonIn);
                break;
            case INSERT:
                message = new Insert(header, myBsonIn);
                break;
            case GET_MORE:
                message = new GetMore(myBsonIn);
                break;
            case DELETE:
                message = new Delete(myBsonIn);
                break;
            case KILL_CURSORS:
                message = new KillCursors(myBsonIn);
                break;

            }

            return message;
        }
        catch (final IOException ioe) {
            throw new MongoDbException(ioe);
        }
    }

    /**
     * Sends a single message to the connection.
     * 
     * @param message
     *            The pending message to send.
     * @throws IOException
     *             On a failure sending the message.
     */
    protected void doSend(final PendingMessage message) throws IOException {
        message.getMessage().write(message.getMessageId(), myBsonOut);
    }

    /**
     * Gets the next id avoiding negative values.
     * 
     * @return The next id.
     */
    protected int nextId() {
        // Get the next id avoiding negative values.
        return (myNextId++ & 0x0FFFFFFF);
    }

    /**
     * Waits for the requested number of messages to become pending.
     * 
     * @param count
     *            The number of pending messages expected.
     * @param millis
     *            The number of milliseconds to wait.
     */
    protected void waitForPending(final int count, final long millis) {
        long now = System.currentTimeMillis();
        final long deadline = now + millis;

        while ((count < myPendingQueue.size()) && (now < deadline)) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            }
            catch (final InterruptedException e) {
                // Ignore.
                e.hashCode();
            }
            now = System.currentTimeMillis();
        }
        // Pause for the write to happen.
        try {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        catch (final InterruptedException e) {
            // Ignore.
            e.hashCode();
        }
    }

    /**
     * Runnable to receive messages.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class ReceiveRunnable implements Runnable {

        @Override
        public void run() {
            while (myOpen.get()) {
                final Message received = doReceive();
                if (received instanceof Reply) {
                    final Reply reply = (Reply) received;
                    final int replyId = reply.getResponseToId();

                    // Keep polling the pending queue until we get to message
                    // based on a matching replyId.
                    PendingMessage nextPending = myPendingQueue.poll();
                    while ((nextPending != null)
                            && (nextPending.getMessageId() != replyId)) {
                        nextPending = myPendingQueue.poll();
                    }

                    if (nextPending != null) {
                        // Must be the pending message's reply.
                        nextPending.reply(reply);
                    }
                    else {
                        LOG.warning("Could not find the Callback for reply '"
                                + replyId + "'.");
                    }
                }
                else {
                    LOG.warning("Received a non-Reply message: " + received);
                }
            }
        }
    }

    /**
     * Runnable to push data out over the MongoDB connection.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class SendRunnable implements Runnable {

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to pull messages off the
         * {@link SocketConnection#myToSendQueue} and push them into the socket
         * connection. If <code>null</code> is ever received from a poll of the
         * queue then the socket connection is flushed and blocking call is made
         * to the queue.
         * </p>
         * 
         * @see Runnable#run()
         */
        @Override
        public void run() {
            boolean needToFlush = false;
            while (myOpen.get()) {
                PendingMessage message = null;
                try {
                    if (needToFlush) {
                        message = myToSendQueue.poll();
                    }
                    else {
                        message = myToSendQueue.take();
                    }

                    if (message != null) {
                        needToFlush = true;
                        // Make sure the message is on the queue before the
                        // message is sent to ensure the receive thread can
                        // assume
                        // an empty pending queue means that there is not
                        // message
                        // for the reply.
                        myPendingQueue.put(message);
                        doSend(message);
                    }
                    else {
                        flush();
                        needToFlush = false;
                    }
                }
                catch (final InterruptedException ie) {
                    // Handled by loop.
                }
                catch (final IOException ioe) {
                    LOG.log(Level.WARNING, "I/O Error sending a message.", ioe);
                    if (message != null) {
                        message.raiseError(ioe);
                    }
                }
            }

            // This may fail because we are dying.
            if (needToFlush) {
                try {
                    flush();
                }
                catch (final IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }
}
