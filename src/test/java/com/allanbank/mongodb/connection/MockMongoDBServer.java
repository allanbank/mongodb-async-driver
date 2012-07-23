/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Header;
import com.allanbank.mongodb.connection.message.Insert;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.util.IOUtils;

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
    private final List<Reply> myReplies = new ArrayList<Reply>();

    /** The requests received. */
    private final List<Message> myRequests = new ArrayList<Message>();

    /** Set to false to stop the server. */
    private boolean myRunning;

    /** The thread acting as the server. */
    private Thread myRunningThread;

    /** The server socket we are listening on. */
    private final ServerSocket myServerSocket;

    /**
     * Creates a new MockMongoDBServer.
     * 
     * @throws IOException
     *             On a failure creating the server socket.
     */
    public MockMongoDBServer() throws IOException {
        super("MockMongoDBServer");

        myServerSocket = new ServerSocket();
        myServerSocket.bind(new InetSocketAddress(InetAddress
                .getByName("127.0.0.1"), 0));

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
        myRunning = false;
        if (myRunningThread != null) {
            myRunningThread.interrupt();
        }
        myServerSocket.close();
    }

    /**
     * Returns the address for the server.
     * 
     * @return The address for the server.
     */
    public InetSocketAddress getInetSocketAddress() {
        return new InetSocketAddress(myServerSocket.getInetAddress(),
                myServerSocket.getLocalPort());
    }

    /**
     * Returns the replies that will be returned after each message is received.
     * 
     * @return the replies to return.
     */
    public List<Reply> getReplies() {
        return Collections.unmodifiableList(myReplies);
    }

    /**
     * Returns the requests that have been received.
     * 
     * @return the requests received.
     */
    public List<Message> getRequests() {
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
        Socket clientSocket = null;

        myRunningThread = Thread.currentThread();
        myRunning = true;
        try {
            while (myRunning) {
                clientSocket = myServerSocket.accept();
                if (clientSocket != null) {
                    try {
                        synchronized (this) {
                            myClientConnected = true;
                            notifyAll();
                        }

                        handleClient(clientSocket);
                    }
                    finally {
                        synchronized (this) {
                            myClientConnected = false;
                            notifyAll();
                        }
                    }
                }
                else {
                    sleep();
                }
            }
        }
        catch (final IOException error) {
            // Exit.
            error.printStackTrace();
        }
    }

    /**
     * Sets the replies to return after each message is received.
     * 
     * @param replies
     *            the replies to send
     */
    public void setReplies(final List<Reply> replies) {
        myReplies.clear();
        if (replies != null) {
            myReplies.addAll(replies);
        }
    }

    /**
     * Sets the replies to return after each message is received.
     * 
     * @param replies
     *            the replies to send
     */
    public void setReplies(final Reply... replies) {
        myReplies.clear();
        if (replies != null) {
            myReplies.addAll(Arrays.asList(replies));
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
     * Handles a single client connection.
     * 
     * @param clientSocket
     *            The socket to receive messages from.
     * 
     * @throws IOException
     *             On a connection error.
     */
    protected void handleClient(final Socket clientSocket) throws IOException {
        InputStream in = null;
        BufferedInputStream buffIn = null;
        BsonInputStream bin = null;

        OutputStream out = null;
        BufferedOutputStream buffOut = null;
        BsonOutputStream bout = null;

        int count = 1;
        try {
            in = clientSocket.getInputStream();
            buffIn = new BufferedInputStream(in);
            bin = new BsonInputStream(buffIn);

            out = clientSocket.getOutputStream();
            buffOut = new BufferedOutputStream(out);
            bout = new BsonOutputStream(buffOut);

            while (myRunning) {
                final Header header = readHeader(bin);
                final Message msg = readMessage(header, bin);

                synchronized (this) {
                    myRequests.add(msg);
                    notifyAll();
                }

                if (!myReplies.isEmpty()) {
                    final Reply reply = myReplies.remove(0);
                    final Reply fixed = new Reply(header.getRequestId(),
                            reply.getCursorId(), reply.getCursorOffset(),
                            reply.getResults(), reply.isAwaitCapable(),
                            reply.isCursorNotFound(), reply.isQueryFailed(),
                            reply.isShardConfigStale());

                    fixed.write(count++, bout);

                    buffOut.flush();
                }
            }
        }
        catch (final EOFException eof) {
            // Client disconnected.
        }
        finally {
            IOUtils.close(buffIn);
            IOUtils.close(in);

            IOUtils.close(buffOut);
            IOUtils.close(out);

            IOUtils.close(clientSocket);
        }
    }

    /**
     * Receives a single message from the connection.
     * 
     * @param bin
     *            The stream to read the message.
     * @return The {@link Message} received.
     * @throws IOException
     *             On an error receiving the message.
     */
    protected Header readHeader(final BsonInputStream bin) throws IOException {
        final int length = bin.readInt();
        final int requestId = bin.readInt();
        final int responseId = bin.readInt();
        final int opCode = bin.readInt();

        final Operation op = Operation.fromCode(opCode);
        if (op == null) {
            // Huh? Dazed and confused
            throw new MongoDbException("Unexpected operation read '" + opCode
                    + "'.");
        }

        return new Header(length, requestId, responseId, op);
    }

    /**
     * Receives a single message from the connection.
     * 
     * @param header
     *            The read message header.
     * @param bin
     *            The stream to read the message.
     * @return The {@link Message} received.
     * @throws IOException
     *             On an error receiving the message.
     */
    protected Message readMessage(final Header header, final BsonInputStream bin)
            throws IOException {
        Message message = null;
        switch (header.getOperation()) {
        case REPLY:
            message = new Reply(header, bin);
            break;
        case QUERY:
            message = new Query(header, bin);
            break;
        case UPDATE:
            message = new Update(bin);
            break;
        case INSERT:
            message = new Insert(header, bin);
            break;
        case GET_MORE:
            message = new GetMore(bin);
            break;
        case DELETE:
            message = new Delete(bin);
            break;
        case KILL_CURSORS:
            message = new KillCursors(bin);
            break;

        }

        return message;

    }

    /**
     * Yawn - go to slepp.
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
