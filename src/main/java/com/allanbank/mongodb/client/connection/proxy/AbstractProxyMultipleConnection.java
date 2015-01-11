/*
 * #%L
 * AbstractProxyMultipleConnection.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.proxy;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * AbstractProxyMultipleConnection provides the core functionality for a
 * connection that multiplexes requests across multiple connections.
 *
 * @param <K>
 *            The key used to track the various connections.
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyMultipleConnection<K>
        implements Connection {

    /** The logger for the {@link AbstractProxyMultipleConnection}. */
    private static final Log LOG = LogFactory
            .getLog(AbstractProxyMultipleConnection.class);

    /** The state of the cluster for finding secondary connections. */
    protected final Cluster myCluster;

    /** The MongoDB client configuration. */
    protected final MongoClientConfiguration myConfig;

    /** Support for emitting property change events. */
    protected final PropertyChangeSupport myEventSupport;

    /** The connection factory for opening secondary connections. */
    protected final ProxiedConnectionFactory myFactory;

    /** The most recently used connection. */
    protected final AtomicReference<Connection> myLastUsedConnection;

    /** The listener for changes in the cluster and connections. */
    protected final PropertyChangeListener myListener;

    /** The primary instance this connection is connected to. */
    protected volatile K myMainKey;

    /** Set to false when the connection is closed. */
    protected final AtomicBoolean myOpen;

    /** Set to true when the connection should be gracefully closed. */
    protected final AtomicBoolean myShutdown;

    /** The servers this connection is connected to. */
    /* package */final ConcurrentMap<K, Connection> myConnections;

    /**
     * Creates a new {@link AbstractProxyMultipleConnection}.
     *
     * @param proxiedConnection
     *            The connection being proxied.
     * @param server
     *            The primary server this connection is connected to.
     * @param cluster
     *            The state of the cluster for finding secondary connections.
     * @param factory
     *            The connection factory for opening secondary connections.
     * @param config
     *            The MongoDB client configuration.
     */
    public AbstractProxyMultipleConnection(final Connection proxiedConnection,
            final K server, final Cluster cluster,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        myMainKey = server;
        myCluster = cluster;
        myFactory = factory;
        myConfig = config;

        myOpen = new AtomicBoolean(true);
        myShutdown = new AtomicBoolean(false);
        myEventSupport = new PropertyChangeSupport(this);
        myConnections = new ConcurrentHashMap<K, Connection>();
        myLastUsedConnection = new AtomicReference<Connection>(
                proxiedConnection);

        myListener = new ClusterAndConnectionListener();
        myCluster.addListener(myListener);

        if (proxiedConnection != null) {
            cacheConnection(server, proxiedConnection);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to add this listener to this connection's event source.
     * </p>
     */
    @Override
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        myEventSupport.addPropertyChangeListener(listener);
    }

    /**
     * Closes the underlying connection.
     *
     * @see Connection#close()
     */
    @Override
    public void close() {

        myOpen.set(false);
        myCluster.removeListener(myListener);

        for (final Connection conn : myConnections.values()) {
            try {
                conn.removePropertyChangeListener(myListener);
                conn.close();
            }
            catch (final IOException ioe) {
                LOG.warn(ioe, "Could not close the connection: {}", conn);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the proxied {@link Connection}.
     * </p>
     *
     * @see java.io.Flushable#flush()
     */
    @Override
    public void flush() throws IOException {
        for (final Connection conn : myConnections.values()) {
            try {
                conn.flush();
            }
            catch (final IOException ioe) {
                LOG.warn(ioe, "Could not flush the connection: {}", conn);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the pending count for the last connection used to
     * send a message.
     * </p>
     */
    @Override
    public int getPendingCount() {
        return myLastUsedConnection.get().getPendingCount();
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
     * Overridden to return if the last used connection is idle.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return myLastUsedConnection.get().isIdle();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return if this connection has any open connections.
     * </p>
     */
    @Override
    public boolean isOpen() {
        return myOpen.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return if the last used connection is shutting down.
     * </p>
     */
    @Override
    public boolean isShuttingDown() {
        return myShutdown.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to raise the errors with all of the underlying connections.
     * </p>
     */
    @Override
    public void raiseErrors(final MongoDbException exception) {
        for (final Connection conn : myConnections.values()) {
            conn.raiseErrors(exception);
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
     * Locates all of the potential servers that can receive all of the
     * messages. Tries to then send the messages to a server with a connection
     * already open or failing that tries to open a connection to open of the
     * servers.
     * </p>
     */
    @Override
    public void send(final Message message1, final Message message2,
            final ReplyCallback replyCallback) throws MongoDbException {

        if (!isAvailable()) {
            throw new ConnectionLostException("Connection shutting down.");
        }

        final List<K> servers = findPotentialKeys(message1, message2);
        if (!trySend(servers, message1, message2, replyCallback)) {
            throw new MongoDbException(
                    "Could not send the messages to any of the potential servers.");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Locates all of the potential servers that can receive all of the
     * messages. Tries to then send the messages to a server with a connection
     * already open or failing that tries to open a connection to open of the
     * servers.
     * </p>
     */
    @Override
    public void send(final Message message, final ReplyCallback replyCallback)
            throws MongoDbException {
        send(message, null, replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to shutdown all of the underlying connections.
     * </p>
     */
    @Override
    public void shutdown(final boolean force) {
        myShutdown.set(true);
        for (final Connection conn : myConnections.values()) {
            conn.shutdown(force);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return getConnectionType() + "(" + myLastUsedConnection.get() + ")";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to wait for all of the underlying connections to close.
     * </p>
     */
    @Override
    public void waitForClosed(final int timeout, final TimeUnit timeoutUnits) {
        final long millis = timeoutUnits.toMillis(timeout);
        long now = System.currentTimeMillis();
        final long deadline = now + millis;

        for (final Connection conn : myConnections.values()) {
            if (now < deadline) {
                conn.waitForClosed((int) (deadline - now),
                        TimeUnit.MILLISECONDS);
                now = System.currentTimeMillis();
            }
        }
    }

    /**
     * Caches the connection to the server if there is not already a connection
     * in the cache. If there is a connection already in the cache then the one
     * provided is closed and the cached connection it returned.
     *
     * @param server
     *            The server connected to.
     * @param conn
     *            The connection to cache, if possible.
     * @return The connection in the cache.
     */
    protected Connection cacheConnection(final K server, final Connection conn) {
        final Connection existing = myConnections.putIfAbsent(server, conn);
        if (existing != null) {
            conn.shutdown(true);
            return existing;
        }

        // Listener to the connection for it to close.
        conn.addPropertyChangeListener(myListener);

        return conn;
    }

    /**
     * Attempts to create a connection to the server, catching any exceptions
     * thrown. If a connection is created it should be
     * {@link #cacheConnection(Object, Connection) cached}.
     *
     * @param server
     *            The server to connect to.
     * @return The connection to the server.
     */
    protected abstract Connection connect(final K server);

    /**
     * Returns the cached connection for the specified key. This method may
     * return {@code null}.
     *
     * @param server
     *            The server connected to.
     * @return The connection in the cache.
     */
    protected Connection connection(final K server) {
        return myConnections.get(server);
    }

    /**
     * Creates a exception for a reconnect failure.
     *
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send.
     * @return The exception.
     */
    protected MongoDbException createReconnectFailure(final Message message1,
            final Message message2) {
        final StringBuilder builder = new StringBuilder(
                "Could not find any servers for the following set of read preferences: ");
        final Set<ReadPreference> seen = new HashSet<ReadPreference>();
        for (final Message message : Arrays.asList(message1, message2)) {
            if (message != null) {
                final ReadPreference prefs = message.getReadPreference();
                if (seen.add(prefs)) {
                    if (seen.size() > 1) {
                        builder.append(", ");
                    }
                    builder.append(prefs);
                }
            }
        }
        builder.append('.');

        return new MongoDbException(builder.toString());
    }

    /**
     * Sends the message on the connection.
     *
     * @param conn
     *            The connection to send on.
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send, may be <code>null</code>.
     * @param reply
     *            The reply {@link Callback}.
     */
    protected void doSend(final Connection conn, final Message message1,
            final Message message2, final ReplyCallback reply) {

        // Use the connection for metrics etc.
        myLastUsedConnection.lazySet(conn);

        if (message2 == null) {
            conn.send(message1, reply);
        }
        else {
            conn.send(message1, message2, reply);
        }
    }

    /**
     * Locates the set of servers that can be used to send the specified
     * messages. This method will attempt to connect to the primary server if
     * there is not a current connection to the primary.
     *
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send. May be <code>null</code>.
     * @return The servers that can be used.
     * @throws MongoDbException
     *             On a failure to locate a server that all messages can be sent
     *             to.
     */
    protected abstract List<K> findPotentialKeys(final Message message1,
            final Message message2) throws MongoDbException;

    /**
     * Returns the type of connection (for logs, etc.).
     *
     * @return The connection type.
     */
    protected abstract String getConnectionType();

    /**
     * Tries to reconnect previously open {@link Connection}s. If a connection
     * was being closed then cleans up the remaining state.
     *
     * @param connection
     *            The connection that was closed.
     */
    protected synchronized void handleConnectionClosed(
            final Connection connection) {

        if (!myOpen.get()) {
            return;
        }

        final K server = findKeyForConnection(connection);

        try {
            // If this is the last connection then go ahead and close this
            // replica set connection so the number of active connections can
            // shrink. Only close this connection on a graceful primary
            // shutdown to pick up when a primary change happens.
            final K primary = myMainKey;
            if ((myConnections.size() == 1)
                    && (!server.equals(primary) || connection.isShuttingDown())) {

                // Mark this a graceful shutdown.
                removeCachedConnection(server, connection);
                shutdown(true);

                myEventSupport.firePropertyChange(Connection.OPEN_PROP_NAME,
                        true, isOpen());
            }
            // If the connection that closed was the primary then we need to
            // reconnect.
            else if (server.equals(primary) && isOpen()) {
                // Not sure who is primary any more.
                myMainKey = null;

                LOG.info("Primary MongoDB Connection closed: {}({}). "
                        + "Will try to reconnect.", getConnectionType(),
                        connection);

                // Need to use the reconnect logic to find the new primary.
                final ConnectionInfo<K> newConn = reconnectMain();
                if (newConn != null) {
                    removeCachedConnection(server, connection);
                    updateMain(newConn);
                }
                // Else could not find a primary. Likely in a bad state but let
                // the connection stay for secondary queries if we have another
                // connection.
                else if (myConnections.size() == 1) {
                    // Mark this a graceful shutdown.
                    removeCachedConnection(server, connection);
                    shutdown(false);

                    myEventSupport.firePropertyChange(
                            Connection.OPEN_PROP_NAME, true, isOpen());
                }
            }
            // Just remove the connection (above).
            else {
                LOG.debug("MongoDB Connection closed: {}({}).",
                        getConnectionType(), connection);
            }
        }
        finally {
            // Make sure we always remove the closed connection.
            removeCachedConnection(server, connection);
            connection.raiseErrors(new ConnectionLostException(
                    "Connection closed."));
        }
    }

    /**
     * Creates a connection back to the main server for this connection.
     *
     * @return The information for the new connection.
     */
    protected abstract ConnectionInfo<K> reconnectMain();

    /**
     * Remove the connection from the cache.
     *
     * @param key
     *            The key to remove the connection for.
     * @param connection
     *            The connection to remove (if known).
     */
    protected void removeCachedConnection(final Object key,
            final Connection connection) {
        Connection conn = connection;
        if (connection == null) {
            conn = myConnections.remove(key);
        }
        else if (!myConnections.remove(key, connection)) {
            // Different connection found.
            conn = null;
        }

        if (conn != null) {
            conn.removePropertyChangeListener(myListener);
            conn.shutdown(true);
        }
    }

    /**
     * Tries to send the messages to the first server with either an open
     * connection or that we can open a connection to.
     *
     * @param servers
     *            The servers the messages can be sent to.
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send. May be <code>null</code>.
     * @param reply
     *            The callback for the replies.
     * @return The true if the message was sent.
     */
    protected boolean trySend(final List<K> servers, final Message message1,
            final Message message2, final ReplyCallback reply) {
        for (final K server : servers) {

            Connection conn = myConnections.get(server);

            // See if we need to create a connection.
            if (conn == null) {
                // Create one.
                conn = connect(server);
            }
            else if (!conn.isAvailable()) {

                removeCachedConnection(server, conn);

                final ReconnectStrategy strategy = myFactory
                        .getReconnectStrategy();
                conn = strategy.reconnect(conn);
                if (conn != null) {
                    conn = cacheConnection(server, conn);
                }
            }

            if (conn != null) {
                doSend(conn, message1, message2, reply);
                return true;
            }
        }

        return false;
    }

    /**
     * Update the state with the new primary server.
     *
     * @param newConn
     *            The new primary server.
     */
    protected void updateMain(final ConnectionInfo<K> newConn) {
        myMainKey = newConn.getConnectionKey();

        // Add the connection to the cache. This also gets the listener
        // attached.
        cacheConnection(newConn.getConnectionKey(), newConn.getConnection());
    }

    /**
     * Finds the server for the connection.
     *
     * @param connection
     *            The connection to remove.
     * @return The K for the connection.
     */
    private K findKeyForConnection(final Connection connection) {
        for (final Map.Entry<K, Connection> entry : myConnections.entrySet()) {
            if (entry.getValue() == connection) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * ClusterListener provides a listener for changes in the cluster.
     *
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class ClusterAndConnectionListener
            implements PropertyChangeListener {
        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward to either the
         * {@link AbstractProxyMultipleConnection#removeCachedConnection(Object, Connection)}
         * or
         * {@link AbstractProxyMultipleConnection#handleConnectionClosed(Connection)}
         * methods.
         * </p>
         *
         * @see PropertyChangeListener#propertyChange
         */
        @Override
        public void propertyChange(final PropertyChangeEvent event) {
            final String propName = event.getPropertyName();
            if (Cluster.SERVER_PROP.equals(propName)
                    && (event.getNewValue() == null)) {
                // A K has been removed. Close the connection.
                removeCachedConnection(event.getOldValue(), null);
            }
            else if (Connection.OPEN_PROP_NAME.equals(event.getPropertyName())
                    && Boolean.FALSE.equals(event.getNewValue())) {
                handleConnectionClosed((Connection) event.getSource());
            }
        }

    }
}
