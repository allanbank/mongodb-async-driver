/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.FutureCallback;
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
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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

    /** The state of the cluster. */
    private final Cluster myCluster;

    /** The type of the cluster. */
    private final ClusterType myClusterType;

    /** The configuration for the connections. */
    private final MongoClientConfiguration myConfig;

    /** The factory for creating connections to the servers. */
    private final ProxiedConnectionFactory myConnectionFactory;

    /** The units for the ping sweep intervals. */
    private volatile TimeUnit myIntervalUnits = TimeUnit.SECONDS;

    /** The interval for a ping sweep across all of the servers. */
    private volatile int myPingSweepInterval = DEFAULT_PING_INTERVAL_SECONDS;

    /** The thread that is pinging the servers for latency. */
    private final Thread myPingThread;

    /** The flag to stop the ping thread. */
    private volatile boolean myRunning;

    /**
     * Creates a new ClusterPinger.
     * 
     * @param cluster
     *            The state of the cluster.
     * @param clusterType
     *            The type of cluster being managed.
     * @param factory
     *            The factory for creating connections to the servers.
     * @param config
     *            The configuration for the connections.
     */
    public ClusterPinger(final Cluster cluster, final ClusterType clusterType,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        super();

        myCluster = cluster;
        myClusterType = clusterType;
        myConnectionFactory = factory;
        myConfig = config;
        myRunning = true;

        myPingThread = myConfig.getThreadFactory().newThread(this);
        myPingThread.setDaemon(true);
        myPingThread.setName("MongoDB Pinger");
        myPingThread.setPriority(Thread.MIN_PRIORITY);
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
     */
    public void initialSweep() {
        final List<Server> servers = myCluster.getServers();
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

                    // Use a server status request to measure latency. It is
                    // a best case since it does not require any locks.
                    final Future<Reply> reply = PINGER.pingAsync(myClusterType,
                            server, conn);
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
                final List<Server> servers = myCluster.getServers();

                final long interval = getIntervalUnits().toMillis(
                        getPingSweepInterval());
                final long perServerSleep = servers.isEmpty() ? interval
                        : interval / servers.size();

                // Sleep a little before starting. We do it first to give
                // tests time to finish without a sweep in the middle
                // causing confusion and delay.
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(perServerSleep));

                for (final Server server : servers) {

                    // Ping the current server.
                    final String name = server.getCanonicalName();
                    Connection conn = null;
                    try {
                        myPingThread.setName("MongoDB Pinger - " + name);

                        conn = myConnectionFactory.connect(server, myConfig);

                        pingAsync(server, conn);

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
     * Performs a ping of the server.
     * <p>
     * This method also serves as an extension point for derived classes to do
     * other periodic work.
     * </p>
     * 
     * @param server
     *            The server to ping.
     * @param conn
     *            The connection to use to ping the server.
     */
    protected void pingAsync(final Server server, final Connection conn) {
        // Ping to update the latency and tags.
        PINGER.pingAsync(myClusterType, server, conn);
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
                final FutureCallback<Reply> future = new ServerUpdateCallback(
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
