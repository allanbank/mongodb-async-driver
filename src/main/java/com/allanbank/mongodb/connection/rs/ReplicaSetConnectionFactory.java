/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterPinger;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.LatencyServerSelector;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides the ability to create connections to a replica-set environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link ReplicaSetConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetConnectionFactory.class.getCanonicalName());

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    private final MongoDbConfiguration myConfig;

    /** Pings the servers in the cluster collecting latency and tags. */
    private final ClusterPinger myPinger;

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
        myClusterState = new ClusterState(config);
        myPinger = new ClusterPinger(myClusterState, ClusterType.REPLICA_SET,
                factory, config);
        for (final String address : config.getServers()) {
            final ServerState state = myClusterState.add(address);

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
        for (final String addr : myConfig.getServers()) {
            Connection conn = null;
            final FutureCallback<Reply> future = new FutureCallback<Reply>();
            try {
                conn = myConnectionFactory.connect(new ServerState(addr),
                        myConfig);
                conn.send(future, new IsMaster());
                final Reply reply = future.get();
                final List<Document> results = reply.getResults();
                if (!results.isEmpty()) {
                    final Document doc = results.get(0);

                    // Replica Sets MUST connect to the primary server.
                    // See if we can add the other servers also.
                    if (myConfig.isAutoDiscoverServers()) {
                        // Pull them all in.
                        final List<StringElement> hosts = doc.queryPath(
                                StringElement.class, "hosts", ".*");
                        for (final StringElement host : hosts) {
                            myClusterState.add(host.getValue());
                        }
                    }

                    // Add and mark the primary as writable.
                    for (final StringElement primary : doc.queryPath(
                            StringElement.class, "primary")) {

                        myClusterState.markWritable(myClusterState.get(primary
                                .getValue()));

                        break;
                    }
                }
            }
            catch (final IOException ioe) {
                LOG.log(Level.WARNING,
                        "I/O error during replica-set bootstrap to " + addr
                                + ".", ioe);
            }
            catch (final MongoDbException me) {
                LOG.log(Level.WARNING,
                        "MongoDB error during replica-set bootstrap to " + addr
                                + ".", me);
            }
            catch (final InterruptedException e) {
                LOG.log(Level.WARNING,
                        "Interrupted during replica-set bootstrap to " + addr
                                + ".", e);
            }
            catch (final ExecutionException e) {
                LOG.log(Level.WARNING, "Error during replica-set bootstrap to "
                        + addr + ".", e);
            }
            finally {
                IOUtils.close(conn, Level.WARNING,
                        "I/O error shutting down replica-set bootstrap connection to "
                                + addr + ".");
            }
        }

        // Last thing is to start the ping of servers. This will get the tags
        // and latencies updated.
        myPinger.initialSweep();
        myPinger.start();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the cluster state and the
     * {@link ProxiedConnectionFactory}.
     * </p>
     */
    @Override
    public void close() {
        IOUtils.close(myPinger);
        IOUtils.close(myClusterState);
        IOUtils.close(myConnectionFactory);
    }

    /**
     * Creates a new connection to the replica set.
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {

        IOException lastError = null;
        for (final ServerState primary : myClusterState.getWritableServers()) {
            try {
                final Connection primaryConn = myConnectionFactory.connect(
                        primary, myConfig);

                return new ReplicaSetConnection(primaryConn, primary,
                        myClusterState, myConnectionFactory, myConfig);
            }
            catch (final IOException e) {
                lastError = e;
            }
        }

        if (lastError != null) {
            throw lastError;
        }

        throw new IOException(
                "Could not determine the primary server in the replica set.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link ClusterType#REPLICA_SET} cluster type.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return ClusterType.REPLICA_SET;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a replica set {@link ReconnectStrategy}.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {

        final ReplicaSetReconnectStrategy strategy = new ReplicaSetReconnectStrategy();

        strategy.setConfig(myConfig);
        strategy.setConnectionFactory(myConnectionFactory);
        strategy.setState(myClusterState);
        strategy.setSelector(new LatencyServerSelector(myClusterState, false));

        return strategy;
    }

    /**
     * Returns the clusterState value.
     * 
     * @return The clusterState value.
     */
    protected ClusterState getClusterState() {
        return myClusterState;
    }
}
