/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.client.callback.NoOpCallback;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyHandler;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.ConnectionMetricsCollector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.error.DocumentToLargeException;
import com.allanbank.mongodb.error.ServerVersionException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * AbstractConnection provides an abstract connection that does not have any
 * implication on how connections are created.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractConnection implements Connection, Receiver {

    /** The length of the message header in bytes. */
    public static final int HEADER_LENGTH = 16;

    /** The connections configuration. */
    protected final MongoClientConfiguration myConfig;

    /** The cache for the encoding of strings. */
    protected final StringEncoderCache myEncoderCache;

    /** Support for emitting property change events. */
    protected final PropertyChangeSupport myEventSupport;

    /** The executor for the responses. */
    protected final Executor myExecutor;

    /** The listener for messages to and from the server. */
    protected final ConnectionMetricsCollector myListener;

    /** The logger for the connection. */
    protected final Log myLog;

    /** Holds if the connection is open. */
    protected final AtomicBoolean myOpen;

    /** The queue of messages sent but waiting for a reply. */
    protected final PendingMessageQueue myPendingQueue;

    /** The open socket. */
    protected final Server myServer;

    /** Set to true when the connection should be gracefully closed. */
    protected final AtomicBoolean myShutdown;

    /** The {@link PendingMessage} used for the local cached copy. */
    private final PendingMessage myPendingMessage = new PendingMessage();

    /**
     * Creates a new AbstractConnection.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param listener
     *            The listener for messages to and from the server.
     * @param encoderCache
     *            Cache used for encoding strings.
     * @param decoderCache
     *            Cache used for decoding strings.
     * @throws IOException
     *             On a failure connecting to the MongoDB server.
     */
    public AbstractConnection(final Server server,
            final MongoClientConfiguration config,
            final ConnectionMetricsCollector listener,
            final StringEncoderCache encoderCache,
            final StringDecoderCache decoderCache) throws IOException {
        super();
        myServer = server;
        myConfig = config;
        myEncoderCache = encoderCache;

        myListener = listener;
        myLog = LogFactory.getLog(getClass());

        myExecutor = config.getExecutor();
        myEventSupport = new PropertyChangeSupport(this);
        myOpen = new AtomicBoolean(false);
        myShutdown = new AtomicBoolean(false);

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
    public int getPendingCount() {
        return myPendingQueue.size();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns the server's name.
     * </p>
     */
    @Override
    public String getServerName() {
        return myServer.getCanonicalName();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the connection is open and not shutting down.
     * </p>
     */
    @Override
    public boolean isAvailable() {
        return isOpen() && !isShuttingDown();
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
     * True if the connection has not been closed.
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
    public void shutdown(final boolean force) {
        // Mark
        myShutdown.set(true);

        if (force) {
            IOUtils.close(this);
        }
        else {
            if (isOpen()) {
                // Force a message with a callback to wake the receiver up.
                send(new IsMaster(), new NoOpCallback());
            }
        }
    }

    /**
     * Starts the connection.
     */
    public abstract void start();

    /**
     * Stops the socket connection by calling {@link #shutdown(boolean)
     * shutdown(false)}.
     */
    public void stop() {
        shutdown(false);
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
     * Returns the executor to use with replies.
     * 
     * @return The executor for replies.
     */
    protected Executor getExecutor() {
        return myExecutor;
    }

    /**
     * Process a single reply.
     * 
     * @param reply
     *            The received reply.
     */
    protected void handleReply(final Reply reply) {
        final int replyId = reply.getResponseToId();
        boolean took = false;

        // Keep polling the pending queue until we get to
        // message based on a matching replyId.
        try {
            took = myPendingQueue.poll(myPendingMessage);
            while (took && (myPendingMessage.getMessageId() != replyId)) {

                final MongoDbException noReply = new MongoDbException(
                        "No reply received.");

                // Note that this message will not get a reply.
                raiseError(noReply, myPendingMessage.getReplyCallback());

                // Keep looking.
                took = myPendingQueue.poll(myPendingMessage);
            }

            if (took) {
                // Must be the pending message's reply.
                reply(reply, myPendingMessage);
            }
            else {
                myLog.warn("Could not find the callback for reply '{}'.",
                        +replyId);
            }
        }
        finally {
            myPendingMessage.clear();
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
        ReplyHandler.raiseError(exception, replyCallback, myExecutor);
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

        final long latency = pendingMessage.latency();

        myListener.receive(getServerName(), reply.getResponseToId(),
                pendingMessage.getMessage(), reply, latency);

        final ReplyCallback callback = pendingMessage.getReplyCallback();
        ReplyHandler.reply(this, reply, callback, getExecutor());
    }

    /**
     * Ensures that the documents in the message do not exceed the maximum size
     * allowed by MongoDB.
     * 
     * @param message1
     *            The message to be sent to the server.
     * @param message2
     *            The second message to be sent to the server.
     * @throws DocumentToLargeException
     *             On a message being too large.
     * @throws ServerVersionException
     *             If one of the messages cannot be sent to the server version.
     */
    protected void validate(final Message message1, final Message message2)
            throws DocumentToLargeException, ServerVersionException {

        final Version serverVersion = myServer.getVersion();
        final int maxBsonSize = myServer.getMaxBsonObjectSize();

        message1.validateSize(maxBsonSize);
        validateVersion(message1, serverVersion);

        if (message2 != null) {
            message2.validateSize(maxBsonSize);
            validateVersion(message1, serverVersion);
        }
    }

    /**
     * Validates that the server we are about to send the message to knows how
     * to handle the message.
     * 
     * @param message
     *            The message to be sent.
     * @param serverVersion
     *            The server version.
     * @throws ServerVersionException
     *             If the messages cannot be sent to the server version.
     */
    private void validateVersion(final Message message,
            final Version serverVersion) throws ServerVersionException {
        final VersionRange range = message.getRequiredVersionRange();
        if ((range != null) && !range.contains(serverVersion)) {
            throw new ServerVersionException(message.getOperationName(), range,
                    serverVersion, message);
        }
    }
}