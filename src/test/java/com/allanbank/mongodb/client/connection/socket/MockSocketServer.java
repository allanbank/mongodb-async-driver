/*
 * #%L
 * MockSocketServer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * Provides a simple single threaded socket server to act as a MongoDB server in
 * tests. The server collects all messages it receives and can be loaded with
 * replies to the requests it receives.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MockSocketServer extends Thread {
    /** An empty Array of bytes. */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** Set to true when a client is connected. */
    private boolean myClientConnected = false;

    /** The current active connection. */
    private SocketChannel myConnection;

    /** The replies to send when a message is received. */
    private final List<byte[]> myReplies = new ArrayList<byte[]>();

    /** The requests received. */
    private final List<byte[]> myRequests = new ArrayList<byte[]>();

    /** Set to false to stop the server. */
    private boolean myRunning;

    /** The server socket we are listening on. */
    private final ServerSocketChannel myServerSocket;

    /**
     * Creates a new MockMongoDBServer.
     * 
     * @throws IOException
     *             On a failure creating the server socket.
     */
    public MockSocketServer() throws IOException {
        super("MockMongoDBServer");

        myServerSocket = ServerSocketChannel.open();
        myServerSocket.socket().bind(
                new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        myServerSocket.configureBlocking(false);

        myRunning = false;
    }

    /**
     * Clears the requests received and replies to send.
     */
    public void clear() {
        myReplies.clear();
        myRequests.clear();
    }

    /**
     * Closes the server socket.
     * 
     * @throws IOException
     *             On a failure closing the server socket.
     */
    public void close() throws IOException {
        myServerSocket.close();
    }

    /**
     * Disconnects any active client..
     * 
     * @return True if a client is connected, false otherwise.
     */
    public boolean disconnectClient() {
        final SocketChannel channel = myConnection;

        IOUtils.close(channel);
        if (channel != null) {
            close(channel.socket());
        }

        return (channel != null);
    }

    /**
     * Returns the address for the server.
     * 
     * @return The address for the server.
     */
    public InetSocketAddress getInetSocketAddress() {
        return new InetSocketAddress(myServerSocket.socket().getInetAddress(),
                myServerSocket.socket().getLocalPort());
    }

    /**
     * Returns the replies that will be returned after each message is received.
     * 
     * @return the replies to return.
     */
    public List<byte[]> getReplies() {
        return Collections.unmodifiableList(myReplies);
    }

    /**
     * Returns the requests that have been received.
     * 
     * @return the requests received.
     */
    public List<byte[]> getRequests() {
        return Collections.unmodifiableList(myRequests);
    }

    /**
     * Returns the address for the server.
     * 
     * @return The address for the server.
     */
    public String getServerName() {
        return ServerNameUtils.normalize(getInetSocketAddress());
    }

    /**
     * Returns if the server is running.
     * 
     * @return the running
     */
    public boolean isRunning() {
        return myRunning;
    }

    /**
     * Runs the server loop waiting for connections and servicing a single
     * client until it exits.
     */
    @Override
    public void run() {
        myRunning = true;
        try {
            while (myRunning) {
                myConnection = myServerSocket.accept();
                if (myConnection != null) {
                    try {
                        handleClient();
                    }
                    finally {
                        synchronized (this) {
                            myClientConnected = false;
                            notifyAll();
                        }

                        myConnection.close();
                        myConnection = null;
                    }
                }
                else {
                    sleep();
                }
            }
        }
        catch (final IOException error) {
            // Exit.
        }
    }

    /**
     * Sets the replies to return after each message is received.
     * 
     * @param replies
     *            the replies to send
     */
    public void setReplies(final List<byte[]> replies) {
        myReplies.clear();
        if (replies != null) {
            myReplies.addAll(replies);
        }
    }

    /**
     * Controls if the server is running.
     * 
     * @param running
     *            the running to set
     */
    public void setRunning(final boolean running) {
        myRunning = running;
    }

    /**
     * Waits for a client to connect.
     * 
     * @param timeout
     *            Time to wait (in milliseconds) for the disconnect.
     * @return True if a client is connected, false on timeout.
     */
    public boolean waitForClient(final long timeout) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeout;

        boolean result = false;
        synchronized (this) {
            while (!myClientConnected && (now < deadline)) {
                try {
                    notifyAll();
                    wait(deadline - now);
                }
                catch (final InterruptedException e) {
                    // Ignored. Handled by while.
                }
                now = System.currentTimeMillis();
            }
            result = myClientConnected;
        }

        return result;
    }

    /**
     * Waits for a client to disconnect.
     * 
     * @param timeout
     *            Time to wait (in milliseconds) for the disconnect.
     * @return True if a client is disconnected, false on timeout.
     */
    public boolean waitForDisconnect(final long timeout) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeout;

        boolean result;
        synchronized (this) {
            while (myClientConnected && (now < deadline)) {
                try {
                    notifyAll();
                    wait(deadline - now);
                }
                catch (final InterruptedException e) {
                    // Ignored. Handled by while.
                }
                now = System.currentTimeMillis();
            }
            result = !myClientConnected;
        }
        return result;
    }

    /**
     * Waits for a client request.
     * 
     * @param count
     *            The number of request to wait for.
     * @param timeout
     *            Time to wait (in milliseconds) for the disconnect.
     * @return True if a client is connected, false on timeout.
     */
    public boolean waitForRequest(final int count, final long timeout) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeout;
        synchronized (this) {
            while ((myRequests.size() < count) && (now < deadline)) {
                try {
                    // Wake up the receive thread.
                    notifyAll();

                    wait(deadline - now);
                }
                catch (final InterruptedException e) {
                    // Ignored. Handled by while.
                }
                now = System.currentTimeMillis();
            }
        }

        return (myRequests.size() >= count);
    }

    /**
     * Closes the {@link Socket} and logs any error. Sockets do not implement
     * {@link Closeable} in Java 6
     * 
     * @param socket
     *            The connection to close. Sockets do not implement
     *            {@link Closeable} in Java 6
     */
    protected void close(final Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            }
            catch (final IOException ignored) {
                // Ignored
            }
        }
    }

    /**
     * Handles a single client connection.
     * 
     * @throws IOException
     *             On a connection error.
     */
    protected void handleClient() throws IOException {
        // Use non-blocking mode so we can pickup when to stop running.
        myConnection.configureBlocking(false);

        ByteBuffer header = ByteBuffer
                .allocate(AbstractConnection.HEADER_LENGTH);
        ByteBuffer body = null;
        int read = 0;
        while (myRunning) {
            read = 0;
            if (myConnection.isConnectionPending()) {
                myConnection.finishConnect();
            }

            if (myConnection.isConnected()) {
                synchronized (this) {
                    myClientConnected = true;
                    notifyAll();
                }
                if (header.hasRemaining()) {
                    read = myConnection.read(header);
                }
                else {
                    if (body == null) {

                        // First 4 bytes are the message length.
                        final ByteBuffer dup = header.duplicate();
                        dup.flip();
                        final int length = EndianUtils.swap(dup.asIntBuffer()
                                .get(0));
                        body = ByteBuffer.allocate(length
                                - AbstractConnection.HEADER_LENGTH);
                    }

                    if (body.hasRemaining()) {
                        read = myConnection.read(body);
                    }
                    else {
                        // Finished a message.
                        header.flip();
                        body.flip();

                        // Make sure backed by an array.
                        final ByteBuffer completeMessage = ByteBuffer
                                .wrap(new byte[header.capacity()
                                        + body.capacity()]);

                        completeMessage.put(header);
                        completeMessage.put(body);
                        synchronized (this) {
                            myRequests.add(completeMessage.array());
                            notifyAll();
                        }
                        // Setup for the next message.
                        header = ByteBuffer
                                .allocate(AbstractConnection.HEADER_LENGTH);
                        body = null;

                        if (!myReplies.isEmpty()) {
                            final byte[] reply = myReplies.remove(0);
                            final ByteBuffer buffer = ByteBuffer.wrap(reply);
                            while (buffer.hasRemaining()) {
                                myConnection.write(buffer);
                            }
                        }
                    }
                }
            }
            else {
                // Disconnected.
                return;
            }

            if (read < 0) {
                return;
            }
            else if (read == 0) {
                sleep();
            }
        }
    }

    /**
     *
     */
    protected void sleep() {
        long now = System.currentTimeMillis();
        final long deadline = now + 5000;

        try {
            synchronized (this) {
                while (now < deadline) {
                    wait(100);
                    now = deadline;
                }
            }
        }
        catch (final InterruptedException e) {
            // Ignore.
        }
    }
}
