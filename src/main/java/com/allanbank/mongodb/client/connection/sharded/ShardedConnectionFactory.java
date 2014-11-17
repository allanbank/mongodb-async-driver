/*
 * #%L
 * ShardedConnectionFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.sharded;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.ClusterStats;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.MongoClientMetrics;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.ClusterPinger;
import com.allanbank.mongodb.client.state.LatencyServerSelector;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerSelector;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Provides the ability to create connections to a shard configuration via
 * mongos servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link ShardedConnectionFactory}. */
    protected static final Log LOG = LogFactory
            .getLog(ShardedConnectionFactory.class);

    /** The state of the cluster. */
    protected final Cluster myCluster;

    /** The MongoDB client configuration. */
    protected final MongoClientConfiguration myConfig;

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** Pings the servers in the cluster collecting latency and tags. */
    protected final ClusterPinger myPinger;

    /** The selector for the mongos instance to use. */
    protected final ServerSelector mySelector;

    /**
     * Creates a new {@link ShardedConnectionFactory}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param config
     *            The initial configuration.
     */
    public ShardedConnectionFactory(final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        myConnectionFactory = factory;
        myConfig = config;
        myCluster = createCluster(config);
        mySelector = createSelector();
        myPinger = createClusterPinger(factory, config);

        // Add all of the servers to the cluster.
        for (final InetSocketAddress address : config.getServerAddresses()) {
            myCluster.add(address);
        }

        bootstrap();
    }

    /**
     * Finds the mongos servers.
     */
    public void bootstrap() {
        final BootstrapState state = createBootstrapState();
        if (!state.done()) {
            for (final Server addr : myCluster.getServers()) {
                Connection conn = null;
                try {
                    // Send the request...
                    conn = myConnectionFactory.connect(addr, myConfig);

                    update(state, conn);

                    if (state.done()) {
                        break;
                    }
                }
                catch (final IOException ioe) {
                    LOG.warn(ioe, "I/O error during sharded bootstrap to {}.",
                            addr);
                }
                catch (final MongoDbException me) {
                    LOG.warn(me,
                            "MongoDB error during sharded bootstrap to {}.",
                            addr);
                }
                catch (final InterruptedException e) {
                    LOG.warn(e, "Interrupted during sharded bootstrap to {}.",
                            addr);
                }
                catch (final ExecutionException e) {
                    LOG.warn(e, "Error during sharded bootstrap to {}.", addr);
                }
                finally {
                    IOUtils.close(conn, Level.WARNING,
                            "I/O error shutting down sharded bootstrap connection to "
                                    + addr + ".");
                }
            }
        }

        // Last thing is to start the ping of servers. This will get the tags
        // and latencies updated.
        myPinger.initialSweep(myCluster);
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
     * Creates a new connection to the shared mongos servers.
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {
        IOException lastError = null;
        for (final Server server : mySelector.pickServers()) {
            try {
                final Connection primaryConn = myConnectionFactory.connect(
                        server, myConfig);

                return wrap(primaryConn, server);
            }
            catch (final IOException e) {
                lastError = e;
            }
        }

        if (lastError != null) {
            throw lastError;
        }

        throw new IOException(
                "Could not determine a shard server to connect to.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the {@link Cluster}.
     * </p>
     */
    @Override
    public ClusterStats getClusterStats() {
        return myCluster;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link ClusterType#SHARDED} cluster type.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return ClusterType.SHARDED;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the metrics agent from the proxied connection factory.
     * </p>
     */
    @Override
    public MongoClientMetrics getMetrics() {
        return myConnectionFactory.getMetrics();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the delegates strategy but replace his state and
     * selector with our own.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        final ReconnectStrategy delegates = myConnectionFactory
                .getReconnectStrategy();

        delegates.setState(myCluster);
        delegates.setSelector(mySelector);
        delegates.setConnectionFactory(myConnectionFactory);

        return delegates;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets the metrics agent on the proxied connection factory.
     * </p>
     */
    @Override
    public void setMetrics(final MongoClientMetrics metrics) {
        myConnectionFactory.setMetrics(metrics);
    }

    /**
     * Creates a new {@link BootstrapState}.
     * 
     * @return The {@link BootstrapState} to track state of loading the cluster
     *         information.
     */
    protected BootstrapState createBootstrapState() {
        return new BootstrapState(!myConfig.isAutoDiscoverServers());
    }

    /**
     * Creates a {@link Cluster} object to track the state of the servers across
     * the cluster.
     * 
     * @param config
     *            The configuration for the cluster.
     * @return The {@link Cluster} to track the servers across the cluster.
     */
    protected Cluster createCluster(final MongoClientConfiguration config) {
        return new Cluster(config, ClusterType.SHARDED);
    }

    /**
     * Creates a {@link ClusterPinger} object to periodically update the status
     * of the servers.
     * 
     * @param factory
     *            The factory for creating the connections to the servers.
     * @param config
     *            The configuration for the client.
     * 
     * @return The {@link ClusterPinger} object to periodically update the
     *         status of the servers.
     */
    protected ClusterPinger createClusterPinger(
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        return new ClusterPinger(myCluster, factory, config);
    }

    /**
     * Creates a {@link ServerSelector} object to select the (presumed) optimal
     * server to handle a request.
     * <p>
     * For a sharded cluster this defaults to the {@link LatencyServerSelector}.
     * </p>
     * 
     * @return The {@link ServerSelector} object to select the (presumed)
     *         optimal server to handle a request.
     */
    protected ServerSelector createSelector() {
        return new LatencyServerSelector(myCluster, true);
    }

    /**
     * Performs a find on the <tt>config</tt> database's <tt>mongos</tt>
     * collection to return the id for all of the mongos servers in the cluster.
     * <p>
     * A single mongos entry looks like: <blockquote>
     * 
     * <pre>
     * <code>
     * {
     *     "_id" : "mongos.example.com:27017",
     *     "ping" : ISODate("2011-12-05T23:54:03.122Z"),
     *     "up" : 330
     * }
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param conn
     *            The connection to request from.
     * @return True if the configuration servers have been determined.
     * @throws ExecutionException
     *             On a failure to recover the response from the server.
     * @throws InterruptedException
     *             On a failure to receive a response from the server.
     */
    protected boolean findMongosServers(final Connection conn)
            throws InterruptedException, ExecutionException {
        boolean found = false;

        // Create a query to pull all of the mongos servers out of the
        // config database.
        Message message = new Query("config", "mongos", Find.ALL,
        /* fields= */null, /* batchSize= */0,
        /* limit= */0, /* numberToSkip= */0, /* tailable= */false,
                ReadPreference.PRIMARY, /* noCursorTimeout= */false,
                /* awaitData= */false, /* exhaust= */false, /* partial= */
                false);

        while (message != null) {
            // Send the request...
            final FutureReplyCallback future = new FutureReplyCallback();
            conn.send(message, future);

            // Don's send it again.
            message = null;

            // Receive the response.
            final Reply reply = future.get();

            // Validate and pull out the response information.
            final List<Document> docs = reply.getResults();
            for (final Document doc : docs) {
                final Element idElem = doc.get("_id");
                if (idElem instanceof StringElement) {
                    final StringElement id = (StringElement) idElem;

                    myCluster.add(id.getValue());
                    found = true;

                    LOG.debug("Adding shard mongos: {}", id.getValue());
                }
            }

            // Cursor?
            if (reply.getCursorId() != 0) {
                // Send a GetMore.
                message = new GetMore("config", "mongos", reply.getCursorId(),
                        0, ReadPreference.PRIMARY);
            }
        }

        return found;
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
     * Queries for the addresses for the {@code mongos} servers via the
     * {@link #findMongosServers(Connection)} method.
     * 
     * @param state
     *            The state of the bootstrap to be updated.
     * @param conn
     *            The connection to use to locate the {@code mongos} servers
     * @throws InterruptedException
     *             On a failure to wait for the reply to the query due to the
     *             thread being interrupted.
     * @throws ExecutionException
     *             On a failure to execute the query.
     */
    protected void update(final BootstrapState state, final Connection conn)
            throws InterruptedException, ExecutionException {
        if (state.isMongosFound() || findMongosServers(conn)) {
            state.setMongosFound(true);
        }
    }

    /**
     * Wraps the connection in a shard-aware connection.
     * 
     * @param primaryConn
     *            The primary shard connection.
     * @param server
     *            The server the connection is connected to.
     * @return The wrapped connection.
     */
    protected Connection wrap(final Connection primaryConn, final Server server) {
        return new ShardedConnection(primaryConn, server, myCluster,
                mySelector, myConnectionFactory, myConfig);
    }

    /**
     * BootstrapState provides the ability to track the state of the bootstrap
     * for the sharded cluster.
     * 
     * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static class BootstrapState {
        /** Tracks if the {@code mongos} servers have been located. */
        private boolean myMongosFound;

        /**
         * Creates a new BootstrapState.
         * 
         * @param mongosFound
         *            Initials if we should look for the {@code mongos} servers.
         */
        protected BootstrapState(final boolean mongosFound) {
            myMongosFound = mongosFound;
        }

        /**
         * Indicates when the bootstrap is complete.
         * <p>
         * This method returns true if auto discovery is turned off or (if on)
         * when all of the {@code mongos} servers have been located.
         * 
         * @return True once the boot strap is complete.
         */
        public boolean done() {
            return myMongosFound;
        }

        /**
         * Returns true if the {@code mongos} servers have been found, false
         * otherwise.
         * 
         * @return True if the {@code mongos} servers have been found, false
         *         otherwise.
         */
        public boolean isMongosFound() {
            return myMongosFound;
        }

        /**
         * Sets if the the {@code mongos} servers have been found.
         * 
         * @param mongosFound
         *            If true, the {@code mongos} servers have been found, false
         *            otherwise.
         */
        public void setMongosFound(final boolean mongosFound) {
            myMongosFound = mongosFound;
        }
    }
}
