/*
 * #%L
 * SocketConnection.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Flushable;
import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.client.callback.AddressAware;
import com.allanbank.mongodb.client.callback.NoOpCallback;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyHandler;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.BuildInfo;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.ConnectionMetricsCollector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportInputBuffer;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.DocumentToLargeException;
import com.allanbank.mongodb.error.ServerVersionException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Provides a connection that is based on a {@link Transport} implementation for
 * the low level reading and writing of messages.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TransportConnection implements Connection, Receiver,
        TransportResponseListener {

    /** The connections configuration. */
    protected final MongoClientConfiguration myConfig;

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

    /** The {@link PendingMessage} used for the local cached copy on receive. */
    private final PendingMessage myReplyPendingMessage = new PendingMessage();

    /** The sequence for serializing sends. */
    private final Sequence mySendSequence;

    /** The transport for sending messages. */
    private Transport<TransportOutputBuffer> myTransport;

    /**
     * Creates a new SocketConnection to a MongoDB server.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param listener
     *            The listener for the metrics on sent and received messages.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public TransportConnection(final Server server,
            final MongoClientConfiguration config,
            final ConnectionMetricsCollector listener) throws SocketException,
            IOException {
        myServer = server;
        myConfig = config;

        myListener = listener;
        myLog = LogFactory.getLog(getClass());

        myExecutor = config.getExecutor();
        myEventSupport = new PropertyChangeSupport(this);
        myOpen = new AtomicBoolean(false);
        myShutdown = new AtomicBoolean(false);

        myPendingQueue = new PendingMessageQueue(
                config.getMaxPendingOperationsPerConnection(),
                config.getLockType());

        mySendSequence = new Sequence(1, myConfig.getLockType());
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
    public void close() throws IOException {
        getTransport().close();
    }

    /**
     * Notification that the underlying transport has closed either upon request
     * or as a result of the remote end closing the connection.
     * 
     * @param error
     *            The error for the closed connection.
     */
    @Override
    public void closed(MongoDbException error) {
        myOpen.set(false);
        myShutdown.set(true);

        raiseErrors(error);

        myEventSupport.firePropertyChange(OPEN_PROP_NAME, true, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden calls {@link Transport#flush()}.
     * </p>
     * 
     * @see Flushable#flush()
     */
    @Override
    public void flush() throws IOException {
        getTransport().flush();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the send and pending queues are empty.
     * </p>
     */
    @Override
    public int getPendingCount() {
        return myPendingQueue.size() + mySendSequence.getWaitersCount();
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
     * Returns the underlying transport.
     *
     * @return The underlying transport.
     */
    public Transport<TransportOutputBuffer> getTransport() {
        return myTransport;
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
        return myPendingQueue.isEmpty() && mySendSequence.isIdle();
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
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "MongoDB(" + myTransport.toString() + ")";
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
     * Handles a response from the server.
     * 
     * @param buffer
     *            The buffer containing the response from the server.
     */
    @Override
    public void response(TransportInputBuffer buffer) {
        try {
            Message message = buffer.read();
            if (message instanceof Reply) {
                handleReply((Reply) message);
            }
            else {
                myLog.warn("Received a non-reply message: {}", message);
                raiseErrors(new MongoDbException(
                        "Received a non-reply message."));
                shutdown(true);
            }
        }
        catch (IOException e) {
            raiseErrors(new MongoDbException(e));
            shutdown(true);
        }
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
        final int size = (message2 == null) ? message1.size() : message1.size()
                + message2.size();
        final long seq = mySendSequence.reserve(count);
        final long end = seq + count;

        PendingMessage pending = new PendingMessage();
        boolean interrupted = Thread.interrupted();
        boolean sawError = false;
        try {
            // Serialize the messages now so the critical section becomes close
            // to a write(byte[]) (with a little accounting overhead).
            TransportOutputBuffer out = getTransport().createSendBuffer(size);

            pending.set((int) (seq & 0xFFFFFF), message1, replyCallback);
            out.write((int) (seq & 0xFFFFFF), message1, replyCallback);
            if (message2 != null) {
                pending.set((int) ((seq + 1) & 0xFFFFFF), message2,
                        replyCallback);
                out.write((int) ((seq + 1) & 0xFFFFFF), message2, replyCallback);
            }

            // Now stand in line.
            mySendSequence.waitFor(seq);

            // Send, quickly now.
            if (!myPendingQueue.offer(pending)) {
                // Flush before blocking.
                flush();
                myPendingQueue.put(pending);
            }
            getTransport().send(out);

            // If no-one is waiting we need to flush the message.
            if (mySendSequence.noWaiter(end)) {
                flush();
            }
        }
        catch (final InterruptedException ie) {
            myLog.warn(ie, "Interrupted sending a message.");
            raiseError(ie, replyCallback);
            sawError = true;
        }
        catch (final IOException ioe) {
            myLog.warn(ioe, "I/O Error sending a message.");
            raiseError(ioe, replyCallback);
            sawError = true;
        }
        catch (final RuntimeException re) {
            myLog.warn(re, "Runtime error sending a message.");
            raiseError(re, replyCallback);
            sawError = true;
        }
        catch (final Error error) {
            myLog.error(error, "Error sending a message.");
            raiseError(error, replyCallback);
            sawError = true;
        }
        finally {
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
                    IOUtils.close(TransportConnection.this);
                }
            }
            else {
                // Count the messages as sent.
                myListener.sent(getServerName(), (int) (seq & 0xFFFFFF),
                        message1);
                if (message2 != null) {
                    myListener.sent(getServerName(),
                            (int) ((seq + 1) & 0xFFFFFF), message2);
                }
            }

            // Reset if the thread was interrupted before being called.
            if (interrupted) {
                Thread.currentThread().interrupt();
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
     * Sets the underlying transport.
     * 
     * @param transport
     *            The underlying transport.
     */
    public void setTransport(Transport<TransportOutputBuffer> transport) {
        myTransport = transport;
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
     * Starts the connections.
     */
    public void start() {

        getTransport().start();
        myOpen.set(true);

        if (myServer.needBuildInfo()) {
            send(new BuildInfo(), new ServerUpdateCallback(myServer));
        }
    }

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
     * Overridden to call {@link Receiver#tryReceive() tryReceive()} on the
     * {@link Transport} if it implements {@link Receiver}.
     * </p>
     * 
     * @see Receiver#tryReceive()
     */
    @Override
    public void tryReceive() {
        if (getTransport() instanceof Receiver) {
            ((Receiver) getTransport()).tryReceive();
        }
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
            took = myPendingQueue.poll(myReplyPendingMessage);
            while (took && (myReplyPendingMessage.getMessageId() != replyId)) {

                final MongoDbException noReply = new MongoDbException(
                        "No reply received.");

                // Note that this message will not get a reply.
                raiseError(noReply, myReplyPendingMessage.getReplyCallback());

                // Keep looking.
                took = myPendingQueue.poll(myReplyPendingMessage);
            }

            if (took) {
                // Must be the pending message's reply.
                reply(reply, myReplyPendingMessage);
            }
            else {
                myLog.warn("Could not find the callback for reply '{}'.",
                        Integer.valueOf(replyId));
            }
        }
        finally {
            myReplyPendingMessage.clear();
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
