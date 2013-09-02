/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
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
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.PendingMessageQueue;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ReplyHandler;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * AbstractSocketConnection provides the basic functionality for a socket
 * connection that passes messages between the sender and receiver.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractSocketConnection implements Connection {
    /** The length of the message header in bytes. */
    public static final int HEADER_LENGTH = 16;

    /** The writer for BSON documents. Shares this objects {@link #myInput}. */
    protected final BsonInputStream myBsonIn;

    /** The writer for BSON documents. Shares this objects {@link #myOutput}. */
    protected final BsonOutputStream myBsonOut;

    /** Support for emitting property change events. */
    protected final PropertyChangeSupport myEventSupport;

    /** The executor for the responses. */
    protected final Executor myExecutor;

    /** The buffered input stream. */
    protected final InputStream myInput;

    /** The logger for the connection. */
    protected final Logger myLog;

    /** Holds if the connection is open. */
    protected final AtomicBoolean myOpen;

    /** The buffered output stream. */
    protected final BufferedOutputStream myOutput;

    /** The queue of messages sent but waiting for a reply. */
    protected final PendingMessageQueue myPendingQueue;

    /** The open socket. */
    protected final ServerState myServer;

    /** Set to true when the connection should be gracefully closed. */
    protected final AtomicBoolean myShutdown;

    /** The open socket. */
    protected final Socket mySocket;

    /** The number of messages sent by the connection. */
    private final AtomicLong myMessagesSent;

    /** The number of messages received by the connection. */
    private final AtomicLong myRepliesReceived;

    /**
     * The total amount of time messages waited for a reply from the server in
     * nanoseconds.
     */
    private final AtomicLong myTotalLatencyNanoSeconds;

    /**
     * Creates a new AbstractSocketConnection.
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
    public AbstractSocketConnection(final ServerState server,
            final MongoClientConfiguration config) throws SocketException,
            IOException {
        super();

        myLog = Logger.getLogger(getClass().getCanonicalName());

        myExecutor = config.getExecutor();
        myServer = server;
        myEventSupport = new PropertyChangeSupport(this);
        myOpen = new AtomicBoolean(false);
        myShutdown = new AtomicBoolean(false);

        myRepliesReceived = new AtomicLong(0);
        myTotalLatencyNanoSeconds = new AtomicLong(0);
        myMessagesSent = new AtomicLong(0);

        mySocket = config.getSocketFactory().createSocket();
        mySocket.connect(myServer.getServer(), config.getConnectTimeout());
        updateSocketWithOptions(config);

        myOpen.set(true);

        myInput = mySocket.getInputStream();
        myBsonIn = new BsonInputStream(myInput);

        // Careful with the size of the buffer here. Seems Java like to call
        // madvise(..., MADV_DONTNEED) for buffers over a certain size.
        // Net effect is that the performance of the system goes down the
        // drain. Some numbers using the
        // UnixDomainSocketAccepatanceTest.testMultiFetchiterator
        // 1M ==> More than a minute...
        // 512K ==> 24 seconds
        // 256K ==> 16.9 sec.
        // 128K ==> 17 sec.
        // 64K ==> 17 sec.
        // 32K ==> 16.5 sec.
        // Based on those numbers we set the buffer to 32K as larger does not
        // improve performance.
        myOutput = new BufferedOutputStream(mySocket.getOutputStream(),
                32 * 1024);
        myBsonOut = new BsonOutputStream(myOutput);

        myPendingQueue = new PendingMessageQueue(
                config.getMaxPendingOperationsPerConnection(),
                config.getLockType());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to add the listener to this connection.
     * </p>
     */
    @Override
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        myEventSupport.addPropertyChangeListener(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        myOutput.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessagesSent() {
        return myMessagesSent.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPendingCount() {
        return myPendingQueue.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getRepliesReceived() {
        return myRepliesReceived.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns the server's name.
     * </p>
     */
    @Override
    public String getServerName() {
        return myServer.getServer().getHostName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalLatencyNanoSeconds() {
        return myTotalLatencyNanoSeconds.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the send and pending queues are empty.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return myPendingQueue.isEmpty();
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
    public boolean isShuttingDown() {
        return myShutdown.get();
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

        while (myPendingQueue.poll(message)) {
            raiseError(exception, message.getReplyCallback());
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to remove the listener from this connection.
     * </p>
     */
    @Override
    public void removePropertyChangeListener(
            final PropertyChangeListener listener) {
        myEventSupport.removePropertyChangeListener(listener);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to mark the socket as shutting down and tickles the sender to
     * make sure that happens as soon as possible.
     * </p>
     */
    @Override
    public void shutdown() {
        // Mark
        myShutdown.set(true);

        // Force a message with a callback to wake the sender and receiver up.
        send(new IsMaster(), new NoopCallback());
    }

    /**
     * Starts the connection.
     */
    public abstract void start();

    /**
     * Stops the socket connection by calling {@link #shutdown()}.
     */
    public void stop() {
        shutdown();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "MongoDB(" + mySocket.getLocalPort() + "-->"
                + mySocket.getRemoteSocketAddress() + ")";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Waits for the connections pending queues to empty.
     * </p>
     */
    @Override
    public void waitForClosed(final int timeout, final TimeUnit timeoutUnits) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeoutUnits.toMillis(timeout);

        while (isOpen() && (now < deadline)) {
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

            myBsonIn.prefetch(length - 4);

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

            myRepliesReceived.incrementAndGet();

            return message;
        }
        catch (final IOException ioe) {
            final MongoDbException error = new ConnectionLostException(ioe);

            shutdown(error);

            throw error;
        }
    }

    /**
     * Sends a single message to the connection.
     * 
     * @param messageId
     *            The id to use for the message.
     * @param message
     *            The message to send.
     * @throws IOException
     *             On a failure sending the message.
     */
    protected void doSend(final int messageId, final Message message)
            throws IOException {
        message.write(messageId, myBsonOut);
        myMessagesSent.incrementAndGet();
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
            final Callback<Reply> replyCallback) {
        ReplyHandler.raiseError(exception, replyCallback, myExecutor);
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
     * Updates to set the reply for the callback, if any.
     * 
     * @param reply
     *            The reply.
     * @param pendingMessage
     *            The pending message.
     */
    protected void reply(final Reply reply, final PendingMessage pendingMessage) {

        // Add the latency.
        myTotalLatencyNanoSeconds.addAndGet(pendingMessage.latency());

        final Callback<Reply> callback = pendingMessage.getReplyCallback();
        ReplyHandler.reply(reply, callback, myExecutor);
    }

    /**
     * Sends a single message.
     * 
     * @param pendingMessage
     *            The message to be sent.
     * 
     * @throws InterruptedException
     *             If the thread is interrupted waiting for a message to send.
     * @throws IOException
     *             On a failure sending the message.
     */
    protected final void send(final PendingMessage pendingMessage)
            throws InterruptedException, IOException {
        final int messageId = pendingMessage.getMessageId();
        final Message message = pendingMessage.getMessage();

        // Make sure the message is on the queue before the
        // message is sent to ensure the receive thread can
        // assume an empty pending queue means that there is
        // no message for the reply.
        if ((pendingMessage.getReplyCallback() != null)
                && !myPendingQueue.offer(pendingMessage)) {
            // Push what we have out before blocking.
            flush();
            myPendingQueue.put(pendingMessage);
        }

        doSend(messageId, message);

        // If shutting down then flush after each message.
        if (myShutdown.get()) {
            flush();
        }
    }

    /**
     * Shutsdown the connection on an error.
     * 
     * @param error
     *            The error causing the shutdown.
     */
    protected void shutdown(final MongoDbException error) {
        // Have to assume all of the requests have failed that are pending.
        final PendingMessage message = new PendingMessage();
        while (myPendingQueue.poll(message)) {
            raiseError(error, message.getReplyCallback());
        }

        closeQuietly();
    }

    /**
     * Updates the socket with the configuration's socket options.
     * 
     * @param config
     *            The configuration to apply.
     * @throws SocketException
     *             On a failure setting the socket options.
     */
    protected void updateSocketWithOptions(final MongoClientConfiguration config)
            throws SocketException {
        mySocket.setKeepAlive(config.isUsingSoKeepalive());
        mySocket.setSoTimeout(config.getReadTimeout());
        try {
            mySocket.setTcpNoDelay(true);
        }
        catch (final SocketException seIgnored) {
            // The junixsocket implementation does not support TCP_NO_DELAY,
            // which makes sense but it throws an exception instead of silently
            // ignoring - ignore it here.
            if (!"AFUNIXSocketException".equals(seIgnored.getClass()
                    .getSimpleName())) {
                throw seIgnored;
            }
        }
        mySocket.setPerformancePreferences(1, 5, 6);
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
            myLog.log(Level.WARNING,
                    "I/O exception trying to shutdown the connection.", e);
        }
    }

    /**
     * NoopCallback provides a callback that does not look at the reply.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class NoopCallback implements Callback<Reply> {
        /**
         * {@inheritDoc}
         * <p>
         * Overridden to do nothing.
         * </p>
         */
        @Override
        public void callback(final Reply result) {
            // Noop.
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to do nothing.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            // Noop.
        }
    }

}