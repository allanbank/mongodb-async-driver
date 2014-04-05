/*
 * Copyright 2011-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.ClusterPinger;
import com.allanbank.mongodb.client.state.LatencyServerSelector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Provides the ability to create connections to a replica-set environment.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link ReplicaSetConnectionFactory}. */
    protected static final Log LOG = LogFactory
            .getLog(ReplicaSetConnectionFactory.class);

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final Cluster myCluster;

    /** The MongoDB client configuration. */
    private final MongoClientConfiguration myConfig;

    /** Pings the servers in the cluster collecting latency and tags. */
    private final ClusterPinger myPinger;

    /** The strategy for reconnecting/finding the primary. */
    private final ReplicaSetReconnectStrategy myStrategy;

    /**
     * Creates a new {@link ReplicaSetConnectionFactory}.
     *
     * @param factory
     *            The factory to create proxied connections.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnectionFactory(final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        myConnectionFactory = factory;
        myConfig = config;
        myCluster = new Cluster(config);
        myPinger = new ClusterPinger(myCluster, ClusterType.REPLICA_SET,
                factory, config);

        myStrategy = new ReplicaSetReconnectStrategy();
        myStrategy.setConfig(myConfig);
        myStrategy.setConnectionFactory(myConnectionFactory);
        myStrategy.setState(myCluster);
        myStrategy.setSelector(new LatencyServerSelector(myCluster, false));

        // Bootstrap the state off of one of the servers.
        bootstrap();
    }

    /**
     * Finds the primary member of the replica set.
     */
    public void bootstrap() {
        // To fill in the list of servers.
        locatePrimary();

        // Last thing is to start the ping of servers. This will
        // locate the primary, and get the tags and latencies updated.
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
        IOUtils.close(myConnectionFactory);
    }

    /**
     * Creates a new connection to the replica set.
     *
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {

        // Try to find the primary.
        List<Server> writableServers = myCluster.getWritableServers();
        for (int i = 0; i < 10; ++i) {
            servers: for (final Server primary : writableServers) {
                Connection primaryConn = null;
                try {
                    primaryConn = myConnectionFactory
                            .connect(primary, myConfig);

                    if (isWritable(primary, primaryConn)) {

                        final ReplicaSetConnection rsConnection = new ReplicaSetConnection(
                                primaryConn, primary, myCluster,
                                myConnectionFactory, myConfig, myStrategy);

                        primaryConn = null;

                        return rsConnection;
                    }

                    break servers;
                }
                catch (final IOException e) {
                    LOG.debug(e, "Error connecting to presumptive primary: {}",
                            e.getMessage());
                }
                finally {
                    IOUtils.close(primaryConn);
                }
            }

            // Update the stale state.
            writableServers = locatePrimary();
        }

        // Don't throw an error here.
        // Might be doing a secondary query which means we don't need the
        // primary.
        return new ReplicaSetConnection(null, null, myCluster,
                myConnectionFactory, myConfig, myStrategy);
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
     * Returns the maximum server version within the cluster.
     *
     * @return The maximum server version within the cluster.
     */
    @Override
    public Version getMaximumServerVersion() {
        return myCluster.getMaximumServerVersion();
    }

    /**
     * Returns the minimum server version within the cluster.
     *
     * @return The minimum server version within the cluster.
     */
    @Override
    public Version getMinimumServerVersion() {
        return myCluster.getMinimumServerVersion();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a replica set {@link ReconnectStrategy}.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        return myStrategy;
    }

    /**
     * Returns smallest value for the maximum number of write operations allowed
     * in a single write command.
     *
     * @return The smallest value for maximum number of write operations allowed
     *         in a single write command.
     */
    @Override
    public int getSmallestMaxBatchedWriteOperations() {
        return myCluster.getSmallestMaxBatchedWriteOperations();
    }

    /**
     * Returns the smallest value for the maximum BSON object within the
     * cluster.
     *
     * @return The smallest value for the maximum BSON object within the
     *         cluster.
     */
    @Override
    public long getSmallestMaxBsonObjectSize() {
        return myCluster.getSmallestMaxBsonObjectSize();
    }

    /**
     * Returns the clusterState value.
     *
     * @return The clusterState value.
     */
    protected Cluster getCluster() {
        return myCluster;
    }

    /**
     * Determines if the connection is to the primary member of the cluster.
     *
     * @param server
     *            The server connected to.
     * @param connection
     *            The connection to test.
     * @return True if the connection is to the primary member of the
     *         cluster/replica set.
     */
    protected boolean isWritable(final Server server,
            final Connection connection) {

        try {
            final ServerUpdateCallback replyCallback = new ServerUpdateCallback(
                    server);
            connection.send(new IsMaster(), replyCallback);

            final Reply reply = replyCallback.get();
            final List<Document> results = reply.getResults();
            if (!results.isEmpty()) {
                final Document doc = results.get(0);

                // Get the name of the primary server.
                final StringElement primaryName = doc.get(StringElement.class,
                        "primary");
                if (primaryName != null) {
                    return (primaryName.getValue().equals(connection
                            .getServerName()));
                }
            }
        }
        catch (final InterruptedException e) {
            // Just ignore the reply.
            LOG.debug(e, "Failure testing if a connection is writable: {}",
                    e.getMessage());
        }
        catch (final ExecutionException e) {
            // Just ignore the reply.
            LOG.debug(e, "Failure testing if a connection is writable: {}",
                    e.getMessage());
        }
        return false;
    }

    /**
     * Locates the primary server in the cluster.
     *
     * @return The list of primary servers.
     */
    protected List<Server> locatePrimary() {
        for (final InetSocketAddress addr : myConfig.getServerAddresses()) {
            Connection conn = null;
            final FutureReplyCallback future = new FutureReplyCallback();
            try {
                final Server server = myCluster.add(addr);

                conn = myConnectionFactory.connect(server, myConfig);

                conn.send(new IsMaster(), future);

                final Reply reply = future.get();
                final List<Document> results = reply.getResults();
                if (!results.isEmpty()) {
                    final Document doc = results.get(0);

                    // Replica Sets MUST connect to the primary server.
                    // See if we can add the other servers also.
                    if (myConfig.isAutoDiscoverServers()) {
                        // Pull them all in.
                        final List<StringElement> hosts = doc.find(
                                StringElement.class, "hosts", ".*");
                        for (final StringElement host : hosts) {
                            myCluster.add(host.getValue());
                        }
                    }

                    // Add and mark the primary as writable.
                    final StringElement primary = doc.findFirst(
                            StringElement.class, "primary");
                    if (primary != null) {
                        return Collections.singletonList(myCluster.add(primary
                                .getValue()));
                    }
                }
            }
            catch (final IOException ioe) {
                LOG.warn(ioe, "I/O error during replica-set bootstrap to {}.",
                        addr);
            }
            catch (final MongoDbException me) {
                LOG.warn(me,
                        "MongoDB error during replica-set bootstrap to {}.",
                        addr);
            }
            catch (final InterruptedException e) {
                LOG.warn(e, "Interrupted during replica-set bootstrap to {}.",
                        addr);
            }
            catch (final ExecutionException e) {
                LOG.warn(e, "Error during replica-set bootstrap to {}.", addr);
            }
            finally {
                IOUtils.close(conn, Level.WARNING,
                        "I/O error shutting down replica-set bootstrap connection to "
                                + addr + ".");
            }
        }
        return Collections.emptyList();
    }
}
