/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.bootstrap.BootstrapConnectionFactory;
import com.allanbank.mongodb.connection.messsage.GetLastError;
import com.allanbank.mongodb.connection.messsage.GetMore;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Implementation of the internal {@link Client} interface which all requests to
 * the MongoDB servers pass.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClientImpl implements Client {

    /** The logger for the {@link ClientImpl}. */
    protected static final Logger LOG = Logger.getLogger(ClientImpl.class
            .getCanonicalName());

    /** The configuration for interacting with MongoDB. */
    private final MongoDbConfiguration myConfig;

    /** Factory for creating connections to MongoDB. */
    private final ConnectionFactory myConnectionFactory;

    /** The set of open connections. */
    private final BlockingQueue<Connection> myConnections;

    /** The set of open connections. */
    private final BlockingQueue<Connection> myConnectionsToClose;

    /**
     * Create a new MongoClientConnection.
     * 
     * @param config
     *            The configuration for interacting with MongoDB.
     */
    public ClientImpl(final MongoDbConfiguration config) {
        this(config, new BootstrapConnectionFactory(config));
    }

    /**
     * Create a new MongoClientConnection.
     * 
     * @param config
     *            The configuration for interacting with MongoDB.
     * @param connectionFactory
     *            The source of connection for the client.
     */
    public ClientImpl(final MongoDbConfiguration config,
            final ConnectionFactory connectionFactory) {
        myConfig = config;
        myConnectionFactory = connectionFactory;
        myConnections = new LinkedBlockingQueue<Connection>();
        myConnectionsToClose = new LinkedBlockingQueue<Connection>();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close all of the open connections.
     * </p>
     * 
     * @see Closeable#close()
     */
    @Override
    public void close() {
        // Mark all of the connections as persona non grata.
        myConnections.drainTo(myConnectionsToClose);

        // Work off the connections to close until they are all idle and then
        // closed.
        Connection conn = myConnectionsToClose.poll();
        while (conn != null) {

            conn.waitForIdle(myConfig.getReadTimeout(), TimeUnit.MILLISECONDS);
            close(conn);

            conn = myConnectionsToClose.poll();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configurations default durability.
     * </p>
     * 
     * @see Client#getDefaultDurability()
     */
    @Override
    public Durability getDefaultDurability() {
        return myConfig.getDefaultDurability();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to locate the .
     * </p>
     * 
     * @see Client#send(GetMore,Callback)
     */
    @Override
    public void send(final GetMore getMore, final Callback<Reply> callback) {
        findConnection().send(callback, getMore);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} to MongoDB.
     * </p>
     * 
     * @see Client#send(Message)
     */
    @Override
    public void send(final Message message) {
        findConnection().send(message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} and {@link GetLastError} to
     * MongoDB.
     * </p>
     * 
     * @see Client#send(Message, GetLastError, Callback)
     */
    @Override
    public void send(final Message message, final GetLastError lastError,
            final Callback<Reply> callback) {
        findConnection().send(callback, message, lastError);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Query} to MongoDB.
     * </p>
     * 
     * @see Client#send(Query, Callback)
     */
    @Override
    public void send(final Query query, final Callback<Reply> callback) {
        findConnection().send(callback, query);
    }

    /**
     * Silently closes the connection.
     * 
     * @param conn
     *            The connection to close.
     */
    private void close(final Connection conn) {
        try {
            conn.close();
        }
        catch (final IOException ioe) {
            LOG.log(Level.WARNING, "Error closing connection to MongoDB.", ioe);
        }
    }

    /**
     * Tries to locate a connection that can quickly dispatch the message to a
     * MongoDB server. The basic metrics for determining if a connection is idle
     * is to look at the number of messages waiting to be sent. The basic logic
     * for finding a connection is:
     * <ol>
     * <li>Scan the list of connection looking for an idle connection. If one is
     * found use it.</li>
     * <li>If there are no idle connections determine the maximum number of
     * allowed connections and if there are fewer that the maximum allowed then
     * take the connection creation lock, create a new connection, use it, and
     * add to the set of available connections and release the lock.</li>
     * <li>If there are is still not a connection idle then sort the connections
     * based on a snapshot of pending messages and use the connection with the
     * least messages.</li>
     * <ul>
     * 
     * @return The found connection.
     * @throws MongoDbException
     *             On a failure to talk to the MongoDB servers.
     */
    private Connection findConnection() throws MongoDbException {
        // Make sure we shrink connections when the max changes.
        final int limit = Math.max(1, myConfig.getMaxConnectionCount());
        if (limit < myConnections.size()) {
            synchronized (myConnectionFactory) {
                // Mark the connections as persona non grata.
                while (limit < myConnections.size()) {
                    final Connection conn = myConnections.poll();
                    myConnectionsToClose.add(conn);
                }
            }
        }

        // Locate a connection to use.
        Connection conn = findIdleConnection();
        if (conn == null) {
            conn = tryCreateConnection();
            if (conn == null) {
                conn = findMostIdleConnection();
            }
        }

        // See if any of the connections are ready to close.
        final Connection toClose = myConnectionsToClose.peek();
        if ((toClose != null) && toClose.isIdle()) {
            myConnectionsToClose.remove(toClose);
            close(toClose);
        }

        if (conn == null) {
            throw new MongoDbException(
                    "Could not create a connection to the server.");
        }

        return conn;
    }

    /**
     * Tries to find an idle connection to use.
     * 
     * @return The idle connection, if found.
     */
    private Connection findIdleConnection() {
        for (final Connection conn : myConnections) {
            if (conn.getToBeSentMessageCount() == 0) {
                return conn;
            }
        }

        return null;
    }

    /**
     * Locates the most idle connection to use.
     * 
     * @return The most idle connection.
     */
    private Connection findMostIdleConnection() {
        final SortedMap<Integer, Connection> connections = new TreeMap<Integer, Connection>();
        for (final Connection conn : myConnections) {
            connections.put(Integer.valueOf(conn.getToBeSentMessageCount()),
                    conn);
        }

        if (!connections.isEmpty()) {
            return connections.get(connections.firstKey());
        }
        return null;
    }

    /**
     * Tries to create a new connection.
     * 
     * @return The created connection or null if a connection could not be
     *         created by policy or error.
     */
    private Connection tryCreateConnection() {
        if (myConnections.size() < myConfig.getMaxConnectionCount()) {
            synchronized (myConnectionFactory) {
                final int limit = Math.max(1, myConfig.getMaxConnectionCount());
                if (myConnections.size() < limit) {
                    try {
                        final Connection conn = myConnectionFactory.connect();
                        myConnections.add(conn);

                        return conn;
                    }
                    catch (final IOException ioe) {
                        LOG.log(Level.WARNING,
                                "Could not create a connection.", ioe);
                    }
                }
            }
        }

        return null;
    }
}
