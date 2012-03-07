/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Enumeration;
import java.util.List;
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
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Header;
import com.allanbank.mongodb.connection.message.Insert;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;

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

    /** The writer for BSON documents. Shares this objects {@link #myInput}. */
    private final BsonInputStream myBsonIn;

    /** The writer for BSON documents. Shares this objects {@link #myOutput}. */
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

        int maxMtu = 1000;
        final Enumeration<NetworkInterface> ifaceIter = NetworkInterface
                .getNetworkInterfaces();
        while (ifaceIter.hasMoreElements()) {
            final NetworkInterface iface = ifaceIter.nextElement();
            try {
                maxMtu = Math.max(maxMtu, iface.getMTU());
            }
            catch (final SocketException weTried) {
                // Ignored.
                LOG.fine("Could not determine MTU for "
                        + iface.getDisplayName() + " interface.");
            }
        }
        LOG.fine("Setting socket buffers to " + maxMtu + ".");

        myInput = new BufferedInputStream(mySocket.getInputStream(), maxMtu);
        myBsonIn = new BsonInputStream(myInput);

        myOutput = new BufferedOutputStream(mySocket.getOutputStream(), maxMtu);
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
    public void addPending(final List<PendingMessage> pending) {
        for (final PendingMessage pend : pending) {
            myToSendQueue.add(pend);
        }
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
     * {@inheritDoc}
     */
    @Override
    public void drainPending(final List<PendingMessage> pending) {
        myToSendQueue.drainTo(pending);
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
    public int getPendingCount() {
        return myPendingQueue.size() + myToSendQueue.size();
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
     * <p>
     * True if the send and receive threads are running.
     * </p>
     */
    @Override
    public boolean isOpen() {
        return myOpen.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void send(final Callback<Reply> reply,
            final Message... messages) throws MongoDbException {
        try {
            final int last = messages.length - 1;
            for (int i = 0; i < messages.length; ++i) {
                if (i != last) {
                    myToSendQueue
                            .put(new PendingMessage(nextId(), messages[i]));
                }
                else {
                    myToSendQueue.put(new PendingMessage(nextId(), messages[i],
                            reply));
                }
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
     * Waits for the connections pending queues to empty.
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
            int length;
            try {
                length = readIntSuppressTimeoutOnNonFirstByte();
            }
            catch (final SocketTimeoutException ok) {
                // This is OK. We check if we are still running and come right
                // back.
                return null;
            }

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
            closeQuietly();
            final MongoDbException error = new MongoDbException(ioe);

            // Have to assume all of the requests have failed that are pending.
            PendingMessage msg = myPendingQueue.poll();
            while (msg != null) {
                msg.raiseError(error);

                msg = myPendingQueue.poll();
            }

            throw error;
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
     * Reads a little-endian 4 byte signed integer from the stream.
     * 
     * @return The integer value.
     * @throws EOFException
     *             On insufficient data for the integer.
     * @throws IOException
     *             On a failure reading the integer.
     */
    protected int readIntSuppressTimeoutOnNonFirstByte() throws EOFException,
            IOException {
        int read = 0;
        int eofCheck = 0;
        int result = 0;

        read = myBsonIn.read();
        eofCheck |= read;
        result += (read << 0);

        for (int i = Byte.SIZE; i < Integer.SIZE; i += Byte.SIZE) {
            try {
                read = myBsonIn.read();
            }
            catch (final SocketTimeoutException ste) {
                // Bad - Only the first byte should timeout.
                throw new IOException(ste);
            }
            eofCheck |= read;
            result += (read << i);
        }

        if (eofCheck < 0) {
            throw new EOFException();
        }
        return result;
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
     * Closes the connection to the server without allowing an exception to be
     * thrown.
     */
    private void closeQuietly() {
        try {
            close();
        }
        catch (final IOException e) {
            LOG.log(Level.WARNING,
                    "I/O exception trying to shutdown the connection.", e);
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
                try {
                    final Message received = doReceive();
                    if (received instanceof Reply) {
                        final Reply reply = (Reply) received;
                        final int replyId = reply.getResponseToId();

                        // Keep polling the pending queue until we get to
                        // message
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
                    else if (received != null) {
                        LOG.warning("Received a non-Reply message: " + received);
                    }
                }
                catch (final MongoDbException error) {
                    if (myOpen.get()) {
                        LOG.log(Level.WARNING, "Error reading a message: "
                                + error.getMessage(), error);
                    }
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
                        // assume an empty pending queue means that there is no
                        // message for the reply.
                        if (message.getReplyCallback() != null) {
                            myPendingQueue.put(message);
                        }
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
