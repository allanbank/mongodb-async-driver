/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.bootstrap.BootstrapConnectionFactory;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnection;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * Provides the ability to create connections to a replica-set environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link BootstrapConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetConnectionFactory.class.getCanonicalName());

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    private final MongoDbConfiguration myConfig;

    /**
     * Creates a new {@link ReplicaSetConnectionFactory}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnectionFactory(final ProxiedConnectionFactory factory,
            final MongoDbConfiguration config) {
        myConnectionFactory = factory;
        myConfig = config;
        myClusterState = new ClusterState();
        for (final InetSocketAddress address : config.getServers()) {
            final ServerState state = myClusterState.add(address.getAddress()
                    .getHostName() + ":" + address.getPort());

            // In a replica-set environment we assume that all of the
            // servers are non-writable.
            myClusterState.markNotWritable(state);
        }

        // Bootstrap the state off of one of the servers.
        bootstrap();
    }

    /**
     * Finds the primary member of the replica set.
     */
    public void bootstrap() {
        for (final InetSocketAddress addr : myConfig.getServers()) {
            SocketConnection conn = null;
            final FutureCallback<Reply> future = new FutureCallback<Reply>();
            try {
                conn = new SocketConnection(addr, myConfig);
                conn.send(future, new IsMaster());
                final Reply reply = future.get();
                final List<Document> results = reply.getResults();
                if (!results.isEmpty()) {
                    final Document doc = results.get(0);

                    for (final StringElement primary : doc.queryPath(
                            StringElement.class, "primary")) {

                        myClusterState.markWritable(myClusterState.get(primary
                                .getValue()));

                        return;
                    }
                }
            }
            catch (final IOException ioe) {
                LOG.log(Level.WARNING, "I/O error during bootstrap to " + addr
                        + ".", ioe);
            }
            catch (final InterruptedException e) {
                LOG.log(Level.WARNING, "Interrupted during bootstrap to "
                        + addr + ".", e);
            }
            catch (final ExecutionException e) {
                LOG.log(Level.WARNING, "Error during bootstrap to " + addr
                        + ".", e);
            }
            finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                }
                catch (final IOException okay) {
                    LOG.log(Level.WARNING,
                            "I/O error shutting down bootstrap connection to "
                                    + addr + ".", okay);
                }
            }
        }
    }

    /**
     * Creates a new connection to the replica set.
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {
        return new ReplicaSetConnection(myConnectionFactory, myClusterState,
                myConfig);
    }

}
