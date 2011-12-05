/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * A helper class for constructing connections that are really just proxies on
 * top of other connections.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyConnection implements Connection {
    /** The state of the cluster. */
    protected final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    protected final MongoDbConfiguration myConfig;

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The proxied connection. */
    private Connection myProxiedConnection;

    /**
     * Creates a AbstractProxyConnection.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param clusterState
     *            The state of the cluster.
     * @param config
     *            The MongoDB client configuration.
     */
    public AbstractProxyConnection(final ProxiedConnectionFactory factory,
            final ClusterState clusterState, final MongoDbConfiguration config) {
        myConnectionFactory = factory;
        myClusterState = clusterState;
        myConfig = config;
    }

    /**
     * Closes the underlying connection.
     * 
     * @see Connection#close()
     */
    @Override
    public void close() throws IOException {
        if (myProxiedConnection != null) {
            myProxiedConnection.close();
            myProxiedConnection = null;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link Connection} returned from
     * {@link #ensureConnected()}.
     * </p>
     */
    @Override
    public int send(Message... messages) throws MongoDbException {
        try {
            return ensureConnected().send(messages);
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link Connection} returned from
     * {@link #ensureConnected()}.
     * </p>
     */
    @Override
    public Message receive() throws MongoDbException {
        try {
            return ensureConnected().receive();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link Connection} returned from
     * {@link #ensureConnected()}.
     * </p>
     * 
     * @see java.io.Flushable#flush()
     */
    @Override
    public void flush() throws IOException {
        try {
            ensureConnected().flush();
        }
        catch (final MongoDbException error) {
            onExceptin(error);
            throw error;
        }
    }

    /**
     * Connects to the cluster.
     */
    protected void connect() {
        final List<ServerState> servers = myClusterState.getWritableServers();

        // Shuffle the servers and try to connect to each until one works.
        Exception last = null;
        Collections.shuffle(servers);
        for (final ServerState server : servers) {
            Connection connection = null;
            try {
                connection = myConnectionFactory.connect(server.getServer(),
                        myConfig);

                if (verifyConnection(connection)) {

                    myProxiedConnection = connection;
                    connection = null;

                    return;
                }
            }
            catch (final MongoDbException error) {
                last = error;
            }
            catch (final IOException error) {
                last = error;
            }
            finally {
                if (connection != null) {
                    try {
                        connection.close();
                    }
                    catch (final IOException ignored) {
                        // Nothing.
                    }
                }
            }
        }

        if (last != null) {
            throw new MongoDbException("Could not connect to any server: "
                    + servers, last);
        }
        throw new MongoDbException("Could not connect to any server: "
                + servers);
    }

    /**
     * Ensure that the proxied connection is cconnected and return the
     * Connection to proxy the call to.
     * <p>
     * Checks if already connected. If not creates a new connection to a server.
     * </p>
     * 
     * @return The {@link Connection} to forward a call to.
     * @throws MongoDbException
     *             On a failure establishing a connection.
     */
    protected Connection ensureConnected() throws MongoDbException {
        if (myProxiedConnection == null) {
            connect();
        }
        return myProxiedConnection;
    }

    /**
     * Provides the ability for derived classes to intercept any exceptions from
     * the underlying proxied connection.
     * <p>
     * Closes the underlying connection.
     * </p>
     * 
     * @param exception
     *            The thrown exception.
     */
    protected void onExceptin(final MongoDbException exception) {
        try {
            close();
        }
        catch (final IOException e) {
            myProxiedConnection = null;
        }
    }

    /**
     * Verifies that the connection is working. The connection passed is newly
     * created and can safely be assumed to not have been used for any purposes
     * or have any message sent of received.
     * 
     * @param connection
     *            The connection to verify.
     * @return True if the connection has been verified.
     * @throws MongoDbException
     *             On a failure verifying the connection.
     */
    protected abstract boolean verifyConnection(Connection connection)
            throws MongoDbException;
}
