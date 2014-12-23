/*
 * #%L
 * AbstractSocketTransport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyHandler;
import com.allanbank.mongodb.client.connection.SocketConnectionListener;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * AbstractSocketTransport provides the basic functionality for a socket
 * connection that passes messages between the sender and receiver.
 * 
 * @param <OUT>
 *            The type of the transport's output buffer.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractSocketTransport<OUT extends TransportOutputBuffer>
        implements Transport<OUT> {

    /** The buffered input stream. */
    protected final InputStream myInput;

    /** The logger for the connection. */
    protected final Log myLog;

    /** Holds if the connection is open. */
    protected final AtomicBoolean myOpen;

    /** The buffered output stream. */
    protected final BufferedOutputStream myOutput;

    /** The open socket. */
    protected final Server myServer;

    /** Set to true when the connection should be gracefully closed. */
    protected final AtomicBoolean myShutdown;

    /** The open socket. */
    protected final Socket mySocket;

    /** The writer for BSON documents. Shares this objects {@link #myInput}. */
    private final BsonInputStream myBsonIn;

    /** The listener for responses from the server. */
    protected final TransportResponseListener myResponseListener;

    /**
     * Creates a new AbstractSocketConnection.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param decoderCache
     *            Cache used for decoding strings.
     * @param responseListener
     *            The listener for received messages and changes in the socket.
     * @throws SocketException
     *             On a failure connecting to the MongoDB server.
     * @throws IOException
     *             On a failure to read or write data to the MongoDB server.
     */
    public AbstractSocketTransport(final Server server,
            final MongoClientConfiguration config,
            final StringDecoderCache decoderCache,
            final TransportResponseListener responseListener)
            throws SocketException, IOException {

        myServer = server;
        myResponseListener = responseListener;

        myOpen = new AtomicBoolean(false);
        myShutdown = new AtomicBoolean(false);

        myLog = LogFactory.getLog(getClass());

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
     * <p>
     * Overridden to shutdown the socket cleanly.
     * </p>
     */
    @Override
    public abstract void close() throws IOException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to flush the output stream.
     * </p>
     */
    @Override
    public void flush() throws IOException {
        myOutput.flush();
    }

    /**
     * Returns the bsonIn value.
     *
     * @return The bsonIn value.
     */
    public BsonInputStream getBsonIn() {
        return myBsonIn;
    }

    /**
     * Returns the {@link Socket Socket's}
     * {@link Socket#getRemoteSocketAddress() remote socket address}.
     * 
     * @return The Socket's remote socket address.
     */
    public SocketAddress getRemoteAddress() {
        return mySocket.getRemoteSocketAddress();
    }

    /**
     * Returns the listener for changes in the transport and responses we
     * receive.
     * 
     * @return The listener for changes in the transport and responses we
     *         receive.
     */
    public TransportResponseListener getResponseListener() {
        return myResponseListener;
    }

    /**
     * Returns true if the transport is idle (has not pending work).
     * 
     * @return True if the transport is idle.
     */
    public abstract boolean isIdle();

    /**
     * Returns true if the socket is currently open.
     * 
     * @return True if the socket is currently open.
     */
    public boolean isOpen() {
        return myOpen.get();
    }

    /**
     * Returns true if the connection is being gracefully closed, false
     * otherwise.
     * 
     * @return True if the connection is being gracefully closed, false
     *         otherwise.
     */
    public boolean isShuttingDown() {
        return myShutdown.get();
    }

    /**
     * Notifies the connection that once all outstanding requests have been sent
     * and all replies received the Connection should be closed. This method
     * will return prior to the connection being closed.
     * 
     * @param force
     *            If true then the connection can be immediately closed as the
     *            caller knows there are no outstanding requests to the server.
     */
    public void shutdown(boolean force) {
        // Mark
        myShutdown.set(true);

        if (force) {
            IOUtils.close(this);
        }
        else {
            if (isOpen()) {
                // Force a message with a callback to wake the receiver up.
                try {
                    OUT buffer = createIsMasterBuffer();
                    send(buffer);
                }
                catch (IOException e) {
                    myLog.warn("Could not send a message to wake up the "
                            + "receive thread on a shutdown.", e);
                }
            }
        }
    }

    /**
     * Shuts down the connection on an error.
     * 
     * @param error
     *            The error causing the shutdown.
     * @param receiveError
     *            If true then the socket experienced a receive error.
     */
    public void shutdown(final MongoDbException error,
            final boolean receiveError) {
        if (receiveError) {
            myServer.connectionTerminated();
        }

        closeQuietly();

        myResponseListener.closed(error);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return mySocket.getLocalPort() + "-->"
                + mySocket.getRemoteSocketAddress();
    }

    /**
     * Closes the connection to the server without allowing an exception to be
     * thrown.
     */
    protected void closeQuietly() {
        try {
            close();
        }
        catch (final IOException e) {
            myLog.warn(e, "I/O exception trying to shutdown the connection.");
        }
    }

    /**
     * Creates a {@link TransportOutputBuffer} that contains an isMaster command
     * to wake up the send thread.
     * 
     * @return A buffer containing the isMaster command.
     * @throws IOException
     *             On a failure to create the message.
     */
    protected abstract OUT createIsMasterBuffer() throws IOException;

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
