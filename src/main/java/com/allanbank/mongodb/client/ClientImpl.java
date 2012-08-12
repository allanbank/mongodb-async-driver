/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.bootstrap.BootstrapConnectionFactory;
import com.allanbank.mongodb.error.CannotConnectException;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * Implementation of the internal {@link Client} interface which all requests to
 * the MongoDB servers pass.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClientImpl extends AbstractClient {

    /** The logger for the {@link ClientImpl}. */
    protected static final Logger LOG = Logger.getLogger(ClientImpl.class
            .getCanonicalName());

    /** Counter for the number of reconnects currently being attempted. */
    private int myActiveReconnects;

    /** The configuration for interacting with MongoDB. */
    private final MongoDbConfiguration myConfig;

    /** Factory for creating connections to MongoDB. */
    private final ConnectionFactory myConnectionFactory;

    /** The listener for changes to the state of connections. */
    private final PropertyChangeListener myConnectionListener;

    /** The set of open connections. */
    private final BlockingQueue<Connection> myConnections;

    /** The set of open connections. */
    private final BlockingQueue<Connection> myConnectionsToClose;

    /**
     * Create a new ClientImpl.
     * 
     * @param config
     *            The configuration for interacting with MongoDB.
     */
    public ClientImpl(final MongoDbConfiguration config) {
        this(config, new BootstrapConnectionFactory(config));
    }

    /**
     * Create a new ClientImpl.
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
        myConnectionListener = new ConnectionListener();
        myActiveReconnects = 0;
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
        for (Connection conn = myConnections.poll(); conn != null; conn = myConnections
                .poll()) {
            myConnectionsToClose.add(conn);
            conn.shutdown();
        }

        // Work off the connections to close until they are all closed.
        final List<Connection> conns = new ArrayList<Connection>(
                myConnectionsToClose);
        for (final Connection conn : conns) {
            conn.waitForClosed(myConfig.getReadTimeout(), TimeUnit.MILLISECONDS);
            if (conn.isOpen()) {
                // Force the connection to close.
                close(conn);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configuration used when the client was
     * constructed.
     * </p>
     */
    @Override
    public MongoDbConfiguration getConfig() {
        return myConfig;
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
     * Overridden to return the configurations default read preference.
     * </p>
     * 
     * @see Client#getDefaultReadPreference()
     */
    @Override
    public ReadPreference getDefaultReadPreference() {
        return myConfig.getDefaultReadPreference();
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
     * @param messages
     *            The messages to be sent on the connection. The read preference
     *            for each message should be verified as compatible with the
     *            connection.
     * @return The found connection.
     * @throws MongoDbException
     *             On a failure to talk to the MongoDB servers.
     */
    @Override
    protected Connection findConnection(final Message[] messages)
            throws MongoDbException {
        // Make sure we shrink connections when the max changes.
        final int limit = Math.max(1, myConfig.getMaxConnectionCount());
        if (limit < myConnections.size()) {
            synchronized (myConnectionFactory) {
                // Mark the connections as persona non grata.
                while (limit < myConnections.size()) {
                    final Connection conn = myConnections.poll();
                    myConnectionsToClose.add(conn);
                    conn.shutdown();
                }
            }
        }

        // Locate a connection to use.
        Connection conn = findIdleConnection();
        if (conn == null) {
            conn = tryCreateConnection();
            if (conn == null) {
                conn = findMostIdleConnection();
                if (conn == null) {
                    conn = waitForReconnect(messages);
                }
            }
        }

        if (conn == null) {
            throw new CannotConnectException(
                    "Could not create a connection to the server.");
        }

        return conn;
    }

    /**
     * Tries to reconnect previously open {@link Connection}s. If a connection
     * was being closed then cleans up the remaining state.
     * 
     * @param connection
     *            The connection that was closed.
     */
    protected void handleConnectionClosed(final Connection connection) {
        // Look for the connection in the "active" set first.
        if (myConnections.contains(connection)) {
            // Attempt a reconnect.
            LOG.info("Unexpected MongoDB Connection closed: " + connection
                    + ". Will try to reconnect.");
            reconnect(connection);
        }
        else if (myConnectionsToClose.remove(connection)) {
            LOG.info("MongoDB Connection closed: " + connection);
            connection.removePropertyChangeListener(myConnectionListener);
        }
        else {
            LOG.info("Unknown MongoDB Connection closed: " + connection);
            connection.removePropertyChangeListener(myConnectionListener);
        }
    }

    /**
     * Runs the reconnect logic for the connection.
     * 
     * @param connection
     *            The connection to reconnect.
     */
    protected void reconnect(final Connection connection) {
        final ReconnectStrategy strategy = myConnectionFactory
                .getReconnectStrategy();

        try {
            synchronized (this) {
                myActiveReconnects += 1;
            }

            // Raise errors for all of the pending messages - there is no way to
            // know their state of flight between here and the server.
            MongoDbException exception = new ConnectionLostException(
                    "Connection lost to MongoDB: " + connection);
            connection.raiseErrors(exception, false);

            final Connection newConnection = strategy.reconnect(connection);
            if (newConnection != null) {

                // Get the new connection in the rotation.
                myConnections.remove(connection);
                connection.removePropertyChangeListener(myConnectionListener);

                myConnections.add(newConnection);

            }
            else {
                // Reconnect failed.
                // Raise errors for all of the to be sent and pending messages.
                exception = new CannotConnectException(
                        "Could not reconnect to MongoDB.");
                connection.raiseErrors(exception, true);
            }
        }
        finally {
            synchronized (this) {
                myActiveReconnects -= 1;
                notifyAll();
            }
        }
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
            LOG.log(Level.WARNING, "Error closing connection to MongoDB: "
                    + conn, ioe);
        }
        finally {
            myConnections.remove(conn);
            myConnectionsToClose.remove(conn);

            conn.removePropertyChangeListener(myConnectionListener);
        }
    }

    /**
     * Tries to find an idle connection to use.
     * 
     * @return The idle connection, if found.
     */
    private Connection findIdleConnection() {
        for (final Connection conn : myConnections) {
            if (conn.isOpen() && (conn.getPendingCount() == 0)) {
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
            if (conn.isOpen()) {
                connections.put(Integer.valueOf(conn.getPendingCount()), conn);
            }
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

                        // Add a listener for if the connection is closed.
                        conn.addPropertyChangeListener(myConnectionListener);

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

    /**
     * Checks if there is an active reconnect attempt on-going. If so waits for
     * it to finish (with a timeout) and then searches for a connection again.
     * 
     * @param messages
     *            The messages to be sent on the connection. The read preference
     *            for each message should be verified as compatible with the
     *            connection.
     * @return The connection found after waiting or <code>null</code> if there
     *         was no active reconnect or there was still no connection.
     */
    private Connection waitForReconnect(final Message[] messages) {
        Connection conn = null;
        boolean wasReconnecting = false;
        synchronized (this) {
            wasReconnecting = (myActiveReconnects > 0);
            if (wasReconnecting) {
                long now = System.currentTimeMillis();
                final long deadline = (myConfig.getReconnectTimeout() <= 0) ? Long.MAX_VALUE
                        : now + myConfig.getReconnectTimeout();

                while (now < deadline) {
                    try {
                        LOG.fine("Waiting for reconnect to MongoDB.");
                        wait(deadline - now);

                        now = System.currentTimeMillis();
                    }
                    catch (final InterruptedException e) {
                        // Ignored - Handled by the loop.
                    }
                }
            }
        }

        if (wasReconnecting) {
            // Look again now that we may have reconnected.
            conn = findConnection(messages);
        }
        return conn;
    }

    /**
     * ConnectionListener provides the call back for events occurring on a
     * connection.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected class ConnectionListener implements PropertyChangeListener {

        /**
         * Creates a new ConnectionListener.
         */
        public ConnectionListener() {
            super();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to try reconnecting a connection that has closed.
         * </p>
         */
        @Override
        public void propertyChange(final PropertyChangeEvent event) {
            if (Connection.OPEN_PROP_NAME.equals(event.getPropertyName())
                    && Boolean.FALSE.equals(event.getNewValue())) {
                handleConnectionClosed((Connection) event.getSource());
            }
        }
    }
}
