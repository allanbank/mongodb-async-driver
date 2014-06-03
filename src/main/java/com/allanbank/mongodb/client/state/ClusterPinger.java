/*
 * #%L
 * ClusterPinger.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.ReplicaSetStatus;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * ClusterPinger pings each of the connections in the cluster and updates the
 * latency of the server from this client.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterPinger implements Runnable, Closeable {

    /** The default interval between ping sweeps in seconds. */
    public static final int DEFAULT_PING_INTERVAL_SECONDS = 600;

    /** The logger for the {@link ClusterPinger}. */
    protected static final Log LOG = LogFactory.getLog(ClusterPinger.class);

    /** Instance of the inner class containing the ping logic. */
    private static final Pinger PINGER = new Pinger();

    /**
     * Pings the server and suppresses all exceptions.
     * 
     * @param server
     *            The address of the server. Used for logging.
     * @param conn
     *            The connection to ping.
     * @return True if the ping worked, false otherwise.
     */
    public static boolean ping(final Server server, final Connection conn) {
        return PINGER.ping(server, conn);
    }

    /** The state of the clusters. */
    private final List<Cluster> myClusters;

    /** The configuration for the connections. */
    private final MongoClientConfiguration myConfig;

    /** The factory for creating connections to the servers. */
    private final ProxiedConnectionFactory myConnectionFactory;

    /** The units for the ping sweep intervals. */
    private volatile TimeUnit myIntervalUnits;

    /** The interval for a ping sweep across all of the servers. */
    private volatile int myPingSweepInterval;

    /** The thread that is pinging the servers for latency. */
    private final Thread myPingThread;

    /** The flag to stop the ping thread. */
    private volatile boolean myRunning;

    /**
     * Creates a new ClusterPinger.
     * 
     * @param cluster
     *            The state of the cluster.
     * @param factory
     *            The factory for creating connections to the servers.
     * @param config
     *            The configuration for the connections.
     */
    public ClusterPinger(final Cluster cluster,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        super();

        myConnectionFactory = factory;
        myConfig = config;
        myRunning = true;

        myClusters = new CopyOnWriteArrayList<Cluster>();
        myClusters.add(cluster);

        myIntervalUnits = TimeUnit.SECONDS;
        myPingSweepInterval = DEFAULT_PING_INTERVAL_SECONDS;

        myPingThread = myConfig.getThreadFactory().newThread(this);
        myPingThread.setDaemon(true);
        myPingThread.setName("MongoDB Pinger");
        myPingThread.setPriority(Thread.MIN_PRIORITY);
    }

    /**
     * Adds a new cluster to the set of tracked clusters.
     * 
     * @param cluster
     *            A new cluster to the set of tracked clusters.
     */
    public void addCluster(final Cluster cluster) {
        myClusters.add(cluster);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the pinger.
     * </p>
     */
    @Override
    public void close() {
        myRunning = false;
        myPingThread.interrupt();
    }

    /**
     * Returns the units for the ping sweep intervals.
     * 
     * @return The units for the ping sweep intervals.
     */
    public TimeUnit getIntervalUnits() {
        return myIntervalUnits;
    }

    /**
     * Returns the interval for a ping sweep across all of the servers..
     * 
     * @return The interval for a ping sweep across all of the servers..
     */
    public int getPingSweepInterval() {
        return myPingSweepInterval;
    }

    /**
     * Performs a single sweep through the servers sending a ping with a
     * callback to set the latency and tags for each server.
     * <p>
     * This method will not return until at least 50% of the servers have
     * replied (which may be a failure) to the initial ping.
     * </p>
     * 
     * @param cluster
     *            The cluster of servers to ping.
     */
    public void initialSweep(final Cluster cluster) {
        final List<Server> servers = cluster.getServers();
        final List<Future<Reply>> replies = new ArrayList<Future<Reply>>(
                servers.size());
        final List<Connection> connections = new ArrayList<Connection>(
                servers.size());
        try {
            for (final Server server : servers) {
                // Ping the current server.
                final String name = server.getCanonicalName();
                Connection conn = null;
                try {
                    conn = myConnectionFactory.connect(server, myConfig);

                    // Use a isMaster request to measure latency. It is
                    // a best case since it does not require any locks.
                    final Future<Reply> reply = PINGER.pingAsync(
                            cluster.getType(), server, conn);
                    replies.add(reply);
                }
                catch (final IOException e) {
                    LOG.info("Could not ping '{}': {}", name, e.getMessage());
                }
                finally {
                    if (conn != null) {
                        connections.add(conn);
                        conn.shutdown(false);
                    }
                }
            }

            final int maxRemaining = Math.max(1, replies.size() / 2);
            while (maxRemaining <= replies.size()) {
                final Iterator<Future<Reply>> iter = replies.iterator();
                while (iter.hasNext()) {
                    Future<Reply> future = iter.next();
                    try {
                        if (future != null) {
                            // Pause...
                            future.get(10, TimeUnit.MILLISECONDS);
                        }

                        // A good reply or we could not connect to the server.
                        iter.remove();
                    }
                    catch (final ExecutionException e) {
                        // We got a reply. Its a failure but its a reply.
                        iter.remove();
                    }
                    catch (final TimeoutException e) {
                        // No reply yet.
                        future = null;
                    }
                    catch (final InterruptedException e) {
                        // No reply yet.
                        future = null;
                    }
                }
            }
        }
        finally {
            for (final Connection conn : connections) {
                IOUtils.close(conn);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to periodically wake-up and ping the servers. At first this
     * will occur fairly often but eventually degrade to once every 5 minutes.
     * </p>
     */
    @Override
    public void run() {
        while (myRunning) {
            try {
                final Map<Server, ClusterType> servers = extractAllServers();

                final long interval = getIntervalUnits().toMillis(
                        getPingSweepInterval());
                final long perServerSleep = servers.isEmpty() ? interval
                        : interval / servers.size();

                // Sleep a little before starting. We do it first to give
                // tests time to finish without a sweep in the middle
                // causing confusion and delay.
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(perServerSleep));

                startSweep();

                for (final Map.Entry<Server, ClusterType> entry : servers
                        .entrySet()) {
                    // Ping the current server.
                    final Server server = entry.getKey();
                    final String name = server.getCanonicalName();
                    Connection conn = null;
                    try {
                        myPingThread.setName("MongoDB Pinger - " + name);

                        conn = myConnectionFactory.connect(server, myConfig);

                        PINGER.pingAsync(entry.getValue(), server, conn);

                        // Sleep a little between the servers.
                        Thread.sleep(TimeUnit.MILLISECONDS
                                .toMillis(perServerSleep));
                    }
                    catch (final IOException e) {
                        LOG.info("Could not ping '{}': {}", name,
                                e.getMessage());
                    }
                    finally {
                        myPingThread.setName("MongoDB Pinger - Idle");
                        if (conn != null) {
                            conn.shutdown(true);
                        }
                    }

                }
            }
            catch (final InterruptedException ok) {
                LOG.debug("Pinger interrupted.");
            }
        }
    }

    /**
     * Sets the value of units for the ping sweep intervals.
     * 
     * @param intervalUnits
     *            The new value for the units for the ping sweep intervals.
     */
    public void setIntervalUnits(final TimeUnit intervalUnits) {
        myIntervalUnits = intervalUnits;
    }

    /**
     * Sets the interval for a ping sweep across all of the servers..
     * 
     * @param pingSweepInterval
     *            The new value for the interval for a ping sweep across all of
     *            the servers..
     */
    public void setPingSweepInterval(final int pingSweepInterval) {
        myPingSweepInterval = pingSweepInterval;
    }

    /**
     * Starts the background pinger.
     */
    public void start() {
        myPingThread.start();
    }

    /**
     * Stops the background pinger. Equivalent to {@link #close()}.
     */
    public void stop() {
        close();
    }

    /**
     * Starts the background pinger.
     */
    public void wakeUp() {
        myPingThread.interrupt();
    }

    /**
     * Extension point to notify derived classes that a new sweep is starting.
     */
    protected void startSweep() {
        // Nothing.
    }

    /**
     * Extracts the complete list of servers in all clusters.
     * 
     * @return The complete list of servers across all clusters.
     */
    private Map<Server, ClusterType> extractAllServers() {
        final Map<Server, ClusterType> servers = new HashMap<Server, ClusterType>();

        for (final Cluster cluster : myClusters) {
            for (final Server server : cluster.getServers()) {
                servers.put(server, cluster.getType());
            }
        }

        return Collections.unmodifiableMap(servers);
    }

    /**
     * Pinger provides logic to ping servers.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class Pinger {
        /**
         * Pings the server and suppresses all exceptions. Updates the server
         * state with a latency and the tags found in the response, if any.
         * 
         * @param server
         *            The server to update with the results of the ping. If
         *            <code>false</code> is returned then the state will not
         *            have been updated. Passing <code>null</code> for the state
         *            is allowed.
         * @param conn
         *            The connection to ping.
         * @return True if the ping worked, false otherwise.
         */
        public boolean ping(final Server server, final Connection conn) {
            try {
                final Future<Reply> future = pingAsync(ClusterType.STAND_ALONE,
                        server, conn);

                // Wait for the reply.
                if (future != null) {
                    future.get(1, TimeUnit.MINUTES);

                    return true;
                }
            }
            catch (final ExecutionException e) {
                LOG.info(e, "Could not ping '{}': {}",
                        server.getCanonicalName(), e.getMessage());
            }
            catch (final TimeoutException e) {
                LOG.info(e, "'{}' might be a zombie - not receiving "
                        + "a response to ping: {}", server.getCanonicalName(),
                        e.getMessage());
            }
            catch (final InterruptedException e) {
                LOG.info(e, "Interrupted pinging '{}': {}",
                        server.getCanonicalName(), e.getMessage());
            }

            return false;
        }

        /**
         * Pings the server and suppresses all exceptions. Returns a future that
         * can be used to determine if a response has been received. The future
         * will update the {@link Server} latency and tags if found.
         * 
         * @param type
         *            The type of cluster to ping.
         * @param server
         *            The server to update with the results of the ping. If
         *            <code>false</code> is returned then the state will not
         *            have been updated. Passing <code>null</code> for the state
         *            is allowed.
         * @param conn
         *            The connection to ping.
         * @return A {@link Future} that will be updated once the reply is
         *         received.
         */
        public Future<Reply> pingAsync(final ClusterType type,
                final Server server, final Connection conn) {
            try {
                final ServerUpdateCallback future = new ServerUpdateCallback(
                        server);

                conn.send(new IsMaster(), future);
                if (type == ClusterType.REPLICA_SET) {
                    conn.send(new ReplicaSetStatus(), new ServerUpdateCallback(
                            server));
                }

                return future;
            }
            catch (final MongoDbException e) {
                LOG.info("Could not ping '{}': {}", server, e.getMessage());
            }
            return null;
        }
    }
}
