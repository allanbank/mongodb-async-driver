/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.io.EndianUtils;

/**
 * Provides a simple single threaded socket server to act as a MongoDB server in
 * tests. The server collects all messages it receives and can be loaded with
 * replies to the requests it receives.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MockMongoDBServer extends Thread {
    /** An empty Array of bytes. */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** Set to true when a client is connected. */
    private boolean myClientConnected = false;

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
    public MockMongoDBServer() throws IOException {
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
        SocketChannel clientSocket = null;
        myRunning = true;
        try {
            while (myRunning) {
                clientSocket = myServerSocket.accept();
                if (clientSocket != null) {
                    try {
                        handleClient(clientSocket);
                    }
                    finally {
                        synchronized (this) {
                            myClientConnected = false;
                            notifyAll();
                        }

                        clientSocket.close();
                        clientSocket = null;
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
     * Handles a single client connection.
     * 
     * @param clientSocket
     *            The socket to receive messages from.
     * 
     * @throws IOException
     *             On a connection error.
     */
    protected void handleClient(final SocketChannel clientSocket)
            throws IOException {
        // Use non-blocking mode so we can pickup when to stop running.
        clientSocket.configureBlocking(false);

        ByteBuffer header = ByteBuffer.allocate(SocketConnection.HEADER_LENGTH);
        ByteBuffer body = null;
        int read = 0;
        while (myRunning) {
            read = 0;
            if (clientSocket.isConnectionPending()) {
                clientSocket.finishConnect();
            }

            if (clientSocket.isConnected()) {
                synchronized (this) {
                    myClientConnected = true;
                    notifyAll();
                }
                if (header.hasRemaining()) {
                    read = clientSocket.read(header);
                }
                else {
                    if (body == null) {

                        // First 4 bytes are the message length.
                        final ByteBuffer dup = header.duplicate();
                        dup.flip();
                        final int length = EndianUtils.swap(dup.asIntBuffer()
                                .get(0));
                        body = ByteBuffer.allocate(length
                                - SocketConnection.HEADER_LENGTH);
                    }

                    if (body.hasRemaining()) {
                        read = clientSocket.read(body);
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
                                .allocate(SocketConnection.HEADER_LENGTH);
                        body = null;

                        if (!myReplies.isEmpty()) {
                            final byte[] reply = myReplies.remove(0);
                            final ByteBuffer buffer = ByteBuffer.wrap(reply);

                            while (buffer.hasRemaining()) {
                                clientSocket.write(buffer);
                            }
                        }
                    }
                }
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
                    wait(5000);
                    now = deadline;
                }
            }
        }
        catch (final InterruptedException e) {
            // Ignore.
        }
    }
}
