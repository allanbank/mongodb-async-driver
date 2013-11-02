/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.LatencyServerSelector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerSelector;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.client.state.SimpleReconnectStrategy;

/**
 * {@link ConnectionFactory} to create direct socket connections to the servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactory implements ProxiedConnectionFactory {

    /** The logger for the factory. */
    private static final Logger LOG = Logger
            .getLogger(SocketConnectionFactory.class.getName());

    /** The MongoDB client configuration. */
    private final MongoClientConfiguration myConfig;

    /** The server selector. */
    private final ServerSelector myServerSelector;

    /** The state of the cluster. */
    private final Cluster myState;

    /**
     * Creates a new {@link SocketConnectionFactory}.
     * 
     * @param config
     *            The MongoDB client configuration.
     */
    public SocketConnectionFactory(final MongoClientConfiguration config) {
        super();
        myConfig = config;
        myState = new Cluster(config);
        myServerSelector = new LatencyServerSelector(myState, true);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void close() {
        // Nothing.
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
        final List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>(
                myConfig.getServerAddresses());

        // Shuffle the servers and try to connect to each until one works.
        IOException last = null;
        Collections.shuffle(servers);
        for (final InetSocketAddress address : servers) {
            try {
                final Server server = myState.add(address);
                final Connection conn = connect(server, myConfig);

                // Get the state of the server updated.
                final ServerUpdateCallback cb = new ServerUpdateCallback(server);
                conn.send(new IsMaster(), cb);

                if (Version.UNKNOWN.equals(server.getVersion())) {
                    // If we don't know the version then wait for that response.
                    try {
                        cb.get();
                    }
                    catch (final ExecutionException e) {
                        // Probably not in a good state...
                        LOG.log(Level.FINE,
                                "Could not execute a 'ismaster' command.", e);
                    }
                    catch (final InterruptedException e) {
                        // Probably not in a good state...
                        LOG.log(Level.FINE,
                                "Could not execute a 'ismaster' command.", e);
                    }
                }

                return conn;
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
    public Connection connect(final Server server,
            final MongoClientConfiguration config) throws IOException {

        final AbstractSocketConnection connection;

        switch (myConfig.getConnectionModel()) {
        case SENDER_RECEIVER_THREAD: {
            connection = new TwoThreadSocketConnection(server, myConfig);
            break;
        }
        default: { // and RECEIVER_THREAD
            connection = new SocketConnection(server, myConfig);
            break;
        }
        }

        // Start the connection.
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

    /**
     * Returns the cluster state.
     * 
     * @return The cluster state.
     */
    protected Cluster getState() {
        return myState;
    }
}
