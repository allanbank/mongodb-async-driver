/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.ReplicaSetStatus;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * ClusterPinger pings each of the connections in the cluster and updates the
 * latency of the server from this client.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterPinger implements Runnable, Closeable {

    /** The default interval between ping sweeps in seconds. */
    public static final int DEFAULT_PING_INTERVAL_SECONDS = 600;

    /** The logger for the {@link ClusterPinger}. */
    protected static final Logger LOG = Logger.getLogger(ClusterPinger.class
            .getCanonicalName());

    /** Instance of the inner class containing the ping logic. */
    private static final Pinger PINGER = new Pinger();

    /**
     * Pings the server and suppresses all exceptions.
     * 
     * @param addr
     *            The address of the server. Used for logging.
     * @param conn
     *            The connection to ping.
     * @return True if the ping worked, false otherwise.
     */
    public static boolean ping(final InetSocketAddress addr,
            final Connection conn) {
        return PINGER.ping(addr, conn, null);
    }

    /** The state of the cluster. */
    private final ClusterState myCluster;

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
    public ClusterPinger(final ClusterState cluster,
            final ClusterType clusterType,
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
        final List<ServerState> servers = myCluster.getServers();
        final List<Future<Reply>> replies = new ArrayList<Future<Reply>>(
                servers.size());
        for (final ServerState server : servers) {
            // Ping the current server.
            final String name = server.getName();
            Connection conn = null;
            try {
                // Does the server state have a connection we can use?
                conn = server.takeConnection();
                if (conn == null) {
                    conn = myConnectionFactory.connect(server, myConfig);
                }

                // Use a server status request to measure latency. It is
                // a best case since it does not require any locks.
                final Future<Reply> reply = PINGER.pingAsync(myClusterType,
                        server.getServer(), conn, server);
                replies.add(reply);

                // Give the connection to the server state for reuse.
                if (server.addConnection(conn)) {
                    conn = null;
                }
            }
            catch (final IOException e) {
                LOG.info("Could not ping '" + name + "': " + e.getMessage());
            }
            finally {
                if (conn != null) {
                    conn.shutdown();
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
                final List<ServerState> servers = myCluster.getServers();

                final long interval = getIntervalUnits().toMillis(
                        getPingSweepInterval());
                final long perServerSleep = servers.isEmpty() ? interval
                        : interval / servers.size();

                // Sleep a little before starting. We do it first to give
                // tests time to finish without a sweep in the middle
                // causing confusion and delay.
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(perServerSleep));

                for (final ServerState server : servers) {

                    // Ping the current server.
                    final String name = server.getName();
                    Connection conn = null;
                    try {
                        myPingThread.setName("MongoDB Pinger - " + name);

                        // Does the server state have a connection we can use?
                        conn = server.takeConnection();
                        if (conn == null) {
                            conn = myConnectionFactory
                                    .connect(server, myConfig);
                        }

                        // Ping to update the latency and tags.
                        PINGER.pingAsync(myClusterType, server.getServer(),
                                conn, server);

                        // Give the connection to the server state for reuse.
                        long lastGeneration = 0;
                        ServerState lastServer = null;
                        if (server.addConnection(conn)) {
                            conn = null;
                            lastGeneration = server.getConnectionGeneration();
                            lastServer = server;
                        }

                        // Sleep a little between the servers.
                        Thread.sleep(TimeUnit.MILLISECONDS
                                .toMillis(perServerSleep));

                        // If the last connection has not been used by the state
                        // then it is likely idle and we should cleanup.
                        // Note we just slept for a while.
                        if ((lastServer != null)
                                && (lastGeneration == lastServer
                                        .getConnectionGeneration())) {
                            final Connection lastConn = lastServer
                                    .takeConnection();
                            if (lastConn != null) {
                                lastConn.shutdown();
                            }
                        }
                    }
                    catch (final IOException e) {
                        LOG.info("Could not ping '" + name + "': "
                                + e.getMessage());
                    }
                    finally {
                        myPingThread.setName("MongoDB Pinger - Idle");
                        if (conn != null) {
                            conn.shutdown();
                        }
                    }
                }
            }
            catch (final InterruptedException ok) {
                LOG.fine("Closing pinger on interrupt.");
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
     * Pinger provides logic to ping servers.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class Pinger {
        /**
         * Pings the server and suppresses all exceptions. Updates the server
         * state with a latency and the tags found in the response, if any.
         * 
         * @param addr
         *            The address of the server. Used for logging.
         * @param conn
         *            The connection to ping.
         * @param state
         *            The server state to update with the results of the ping.
         *            If <code>false</code> is returned then the state will not
         *            have been updated. Passing <code>null</code> for the state
         *            is allowed.
         * @return True if the ping worked, false otherwise.
         */
        public boolean ping(final InetSocketAddress addr,
                final Connection conn, final ServerState state) {
            try {
                final Future<Reply> future = pingAsync(ClusterType.STAND_ALONE,
                        addr, conn, state);

                // Wait for the reply.
                if (future != null) {
                    future.get(1, TimeUnit.MINUTES);

                    return true;
                }
            }
            catch (final ExecutionException e) {
                LOG.log(Level.INFO,
                        "Could not ping '" + addr + "': " + e.getMessage(), e);
            }
            catch (final TimeoutException e) {
                LOG.log(Level.INFO, "'" + addr
                        + "' might be a zombie - not receiving "
                        + "a response to ping: " + e.getMessage(), e);
            }
            catch (final InterruptedException e) {
                LOG.log(Level.INFO,
                        "Interrupted pinging '" + addr + "': " + e.getMessage(),
                        e);
            }

            return false;
        }

        /**
         * Pings the server and suppresses all exceptions. Returns a future that
         * can be used to determine if a response has been received. The future
         * will update the {@link ServerState} latency and tags if found.
         * 
         * @param type
         *            The type of cluster to ping.
         * @param addr
         *            The address of the server. Used for logging.
         * @param conn
         *            The connection to ping.
         * @param state
         *            The server state to update with the results of the ping.
         *            If <code>false</code> is returned then the state will not
         *            have been updated. Passing <code>null</code> for the state
         *            is allowed.
         * @return A {@link Future} that will be updated once the reply is
         *         received.
         */
        public Future<Reply> pingAsync(final ClusterType type,
                final InetSocketAddress addr, final Connection conn,
                final ServerState state) {
            try {
                final FutureCallback<Reply> future = new ServerLatencyCallback(
                        state);

                conn.send(new IsMaster(), future);
                if (type == ClusterType.REPLICA_SET) {
                    conn.send(new ReplicaSetStatus(),
                            new SecondsBehindCallback(state));
                }

                return future;
            }
            catch (final MongoDbException e) {
                LOG.info("Could not ping '" + addr + "': " + e.getMessage());
            }
            return null;
        }
    }
}
