/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.LatencyServerSelector;
import com.allanbank.mongodb.connection.state.ServerSelector;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.connection.state.SimpleReconnectStrategy;
import com.allanbank.mongodb.util.IOUtils;

/**
 * {@link ConnectionFactory} to create direct socket connections to the servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactory implements ProxiedConnectionFactory {

    /** The MongoDB client configuration. */
    private final MongoDbConfiguration myConfig;

    /** The server selector. */
    private final ServerSelector myServerSelector;

    /** The state of the cluster. */
    private final ClusterState myState;

    /**
     * Creates a new {@link SocketConnectionFactory}.
     * 
     * @param config
     *            The MongoDB client configuration.
     */
    public SocketConnectionFactory(final MongoDbConfiguration config) {
        super();
        myConfig = config;
        myState = new ClusterState(config);
        myServerSelector = new LatencyServerSelector(myState, true);

        // Add all of the servers as writable by default.
        for (final String address : config.getServers()) {
            final ServerState state = myState.add(address);

            myState.markWritable(state);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the cluster state.
     * </p>
     */
    @Override
    public void close() {
        IOUtils.close(myState);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link SocketConnection}.
     * </p>
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {
        final List<String> servers = new ArrayList<String>(
                myConfig.getServers());

        // Shuffle the servers and try to connect to each until one works.
        IOException last = null;
        Collections.shuffle(servers);
        for (final String address : servers) {
            try {
                return connect(new ServerState(address), myConfig);
            }
            catch (final IOException error) {
                last = error;
            }
        }

        if (last != null) {
            throw last;
        }
        throw new IOException("Could not connect to any server: " + servers);
    }

    /**
     * Creates a connection to the address provided.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @return The Connection to MongoDB.
     * @throws IOException
     *             On a failure connecting to the server.
     */
    @Override
    public Connection connect(final ServerState server,
            final MongoDbConfiguration config) throws IOException {
        final SocketConnection connection = new SocketConnection(server,
                myConfig);

        connection.start();

        return connection;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link ClusterType#STAND_ALONE} cluster type.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return ClusterType.STAND_ALONE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a {@link SimpleReconnectStrategy}.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();
        strategy.setConfig(myConfig);
        strategy.setConnectionFactory(this);
        strategy.setSelector(myServerSelector);
        strategy.setState(myState);

        return strategy;
    }
}
