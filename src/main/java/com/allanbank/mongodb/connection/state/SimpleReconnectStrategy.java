/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.PendingMessage;

/**
 * SimpleReconnectStrategy provides a reconnection strategy to simply attempt to
 * connect to the server again.
 * <p>
 * This class also servers as a base class for more complex strategies.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategy implements ReconnectStrategy {

    /** The configuration for connections to the servers. */
    protected MongoDbConfiguration myConfig = null;

    /** The connection factory for new connections. */
    protected ProxiedConnectionFactory myConnectionFactory = null;

    /** The selector for which server to connect to. */
    protected ServerSelector mySelector = null;

    /** The state of the cluster. */
    protected ClusterState myState = null;

    /**
     * Creates a new SimpleReconnectStrategy.
     */
    public SimpleReconnectStrategy() {
        super();
    }

    /**
     * Returns the configuration for connections to the servers.
     * 
     * @return The configuration for connections to the servers
     */
    public MongoDbConfiguration getConfig() {
        return myConfig;
    }

    /**
     * Returns the connection factory for new connections.
     * 
     * @return The connection factory for new connections.
     */
    public ProxiedConnectionFactory getConnectionFactory() {
        return myConnectionFactory;
    }

    /**
     * Returns the selector for which server to connect to.
     * 
     * @return The selector for which server to connect to.
     */
    public ServerSelector getSelector() {
        return mySelector;
    }

    /**
     * Returns the state of the cluster.
     * 
     * @return The state of the cluster.
     */
    public ClusterState getState() {
        return myState;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to try and connect to every server selected by the
     * ServerSelector until one of the connections is successful.
     * </p>
     */
    @Override
    public Connection reconnect(final Connection oldConnection) {

        final List<ServerState> servers = getSelector().pickServers();

        for (final ServerState server : servers) {
            Connection newConn = null;
            try {
                final InetSocketAddress addr = server.getServer();

                newConn = getConnectionFactory().connect(addr, getConfig());
                if (isConnected(server, newConn)) {
                    copyPending(newConn, oldConnection);

                    return newConn;
                }

                IOUtils.close(newConn);
            }
            catch (final MongoDbException error) {
                IOUtils.close(newConn);
                newConn = null;
            }
            catch (final IOException error) {
                IOUtils.close(newConn);
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(final MongoDbConfiguration config) {
        myConfig = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConnectionFactory(
            final ProxiedConnectionFactory connectionFactory) {
        myConnectionFactory = connectionFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSelector(final ServerSelector selector) {
        mySelector = selector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setState(final ClusterState state) {
        myState = state;
    }

    /**
     * Copies the <tt>oldConnection</tt> pending messages to the
     * <tt>newConnection</tt>.
     * 
     * @param newConnection
     *            The connection to copy the messages to.
     * @param oldConnection
     *            The connection to copy the messages from.
     */
    protected void copyPending(final Connection newConnection,
            final Connection oldConnection) {
        final List<PendingMessage> pending = new ArrayList<PendingMessage>();

        oldConnection.drainPending(pending);
        newConnection.addPending(pending);
    }

    /**
     * Pings the server to verify that the connection is active.
     * 
     * @param server
     *            The server being connected to.
     * @param connection
     *            The connection to verify.
     * @return True if the connection is working, false otherwise.
     */
    protected boolean isConnected(final ServerState server,
            final Connection connection) {
        return ConnectionPinger.ping(server.getServer(), connection);
    }
}
