/*
 * #%L
 * AbstractSocketConnection.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.RandomAccessOutputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.connection.SocketConnectionListener;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Header;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.metrics.ConnectionMetricsCollector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * AbstractSocketConnection provides the basic functionality for a socket
 * connection that passes messages between the sender and receiver.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractSocketConnection extends AbstractConnection {

    /** The writer for BSON documents. Shares this objects {@link #myInput}. */
    protected final BsonInputStream myBsonIn;

    /** The buffered input stream. */
    protected final InputStream myInput;

    /** The buffered output stream. */
    protected final BufferedOutputStream myOutput;

    /** The open socket. */
    protected final Socket mySocket;

    /** Tracks the number of sequential read timeouts. */
    private int myIdleTicks = 0;

    /** Set to true when the sender discovers they are the receive thread. */
    private final AtomicInteger myReaderNeedsToFlush = new AtomicInteger(0);

    /**
     * Creates a new AbstractSocketConnection.
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
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public AbstractSocketConnection(final Server server,
            final MongoClientConfiguration config,
            final ConnectionMetricsCollector listener,
            final StringEncoderCache encoderCache,
            final StringDecoderCache decoderCache) throws SocketException,
            IOException {
        super(server, config, listener, encoderCache, decoderCache);

        mySocket = openSocket(server, config);
        updateSocketWithOptions(config);

        myOpen.set(true);

        myInput = mySocket.getInputStream();
        myBsonIn = new BsonInputStream(myInput, decoderCache);

        // Careful with the size of the buffer here. Seems Java likes to call
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        myReaderNeedsToFlush.set(0);
        myOutput.flush();
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
     * If there is a pending flush then flushes.
     * </p>
     * <p>
     * If there is any available data then does a single receive.
     * </p>
     */
    @Override
    public void tryReceive() {
        try {
            doReceiverFlush();

            if (myBsonIn.available() > 0) {
                doReceiveOne();
            }
        }
        catch (final IOException error) {
            myLog.info(
                    "Received an error when checking for pending messages: {}.",
                    error.getMessage());
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

            return message;
        }

        catch (final IOException ioe) {
            final MongoDbException error = new ConnectionLostException(ioe);

            shutdown(error, (ioe instanceof InterruptedIOException));

            throw error;
        }
    }

    /**
     * Receives and process a single message.
     */
    protected void doReceiveOne() {

        doReceiverFlush();

        final Message received = doReceive();
        if (received instanceof Reply) {
            myIdleTicks = 0;
            final Reply reply = (Reply) received;
            handleReply(reply);
        }
        else if (received != null) {
            myLog.warn("Received a non-Reply message: {}.", received);
            shutdown(new ConnectionLostException(new StreamCorruptedException(
                    "Received a non-Reply message: " + received)), false);
        }
        else {
            myIdleTicks += 1;

            if (myConfig.getMaxIdleTickCount() <= myIdleTicks) {
                // Shutdown the connection., nicely.
                shutdown(false);
            }
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
    protected void doSend(final int messageId,
            final RandomAccessOutputStream message) throws IOException {
        message.writeTo(myOutput);
        message.reset();
    }

    /**
     * Should be called when the send of a message happens on the receive
     * thread. The sender should not flush the {@link #myOutput}. Instead the
     * receive thread will {@link #flush()} once it has consumed all of the
     * pending messages to be received.
     */
    protected void markReaderNeedsToFlush() {
        myReaderNeedsToFlush.incrementAndGet();
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
            throw new EOFException("Remote connection closed: "
                    + mySocket.getRemoteSocketAddress());
        }
        return result;
    }

    /**
     * Sends a single message.
     * 
     * @param pendingMessage
     *            The message to be sent.
     * @param message
     *            The message that has already been encoded/serialized. This may
     *            be <code>null</code> in which case the message is streamed to
     *            the socket.
     * @throws InterruptedException
     *             If the thread is interrupted waiting for a message to send.
     * @throws IOException
     *             On a failure sending the message.
     */
    protected final void send(final PendingMessage pendingMessage,
            final RandomAccessOutputStream message)
            throws InterruptedException, IOException {

        final int messageId = pendingMessage.getMessageId();

        // Mark the timestamp.
        pendingMessage.timestampNow();

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

        myListener
                .sent(getServerName(), messageId, pendingMessage.getMessage());

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
     * @param receiveError
     *            If true then the socket experienced a receive error.
     */
    protected void shutdown(final MongoDbException error,
            final boolean receiveError) {
        if (receiveError) {
            myServer.connectionTerminated();
        }

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
     * Closes the connection to the server without allowing an exception to be
     * thrown.
     */
    private void closeQuietly() {
        try {
            close();
        }
        catch (final IOException e) {
            myLog.warn(e, "I/O exception trying to shutdown the connection.");
        }
    }

    /**
     * Check if the handler for a message dropped data in the send buffer that
     * it did not flush to avoid a deadlock with the server. If so then flush
     * that message.
     */
    private void doReceiverFlush() {
        try {
            final int unflushedMessages = myReaderNeedsToFlush.get();
            if ((unflushedMessages != 0)
                    && (myPendingQueue.size() <= unflushedMessages)) {
                flush();
            }
        }
        catch (final IOException ignored) {
            myLog.warn("Error flushing data to the server: "
                    + ignored.getMessage());
        }
    }

    /**
     * Tries to open a connection to the server.
     * 
     * @param server
     *            The server to open the connection to.
     * @param config
     *            The configuration for attempting to open the connection.
     * @return The opened {@link Socket}.
     * @throws IOException
     *             On a failure opening a connection to the server.
     */
    private Socket openSocket(final Server server,
            final MongoClientConfiguration config) throws IOException {
        final SocketFactory factory = config.getSocketFactory();

        IOException last = null;
        Socket socket = null;
        for (final InetSocketAddress address : myServer.getAddresses()) {
            try {

                socket = factory.createSocket();
                socket.connect(address, config.getConnectTimeout());

                // If the factory wants to know about the connection then let it
                // know first.
                if (factory instanceof SocketConnectionListener) {
                    ((SocketConnectionListener) factory).connected(address,
                            socket);
                }

                // Let the server know the working connection.
                server.connectionOpened(address);

                last = null;
                break;
            }
            catch (final IOException error) {
                last = error;
                try {
                    if (socket != null) {
                        socket.close();
                    }
                }
                catch (final IOException ignore) {
                    myLog.info(
                            "Could not close the defunct socket connection: {}",
                            socket);
                }
            }

        }
        if (last != null) {
            server.connectFailed();
            throw last;
        }

        return socket;
    }
}
