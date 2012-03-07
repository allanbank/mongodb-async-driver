/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * ConnectionPinger pings each of the connections in the cluster and updates the
 * latency of the server from this client.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConnectionPinger implements Runnable, Closeable {

    /** The maximum interval between ping sweeps in seconds. */
    public static final int MAX_PING_INTERVAL_SECONDS = 600;

    /** The minimum interval between ping sweeps in seconds. */
    public static final int MIN_PING_INTERVAL_SECONDS = 30;

    /**
     * The increment for increases in the interval between ping sweeps in
     * seconds.
     */
    public static final int PING_INTERVAL_INCREMENT_SECONDS = 15;

    /** The logger for the {@link ConnectionPinger}. */
    protected static final Logger LOG = Logger.getLogger(ConnectionPinger.class
            .getCanonicalName());

    /** The state of the cluster. */
    private final ClusterState myCluster;

    /** The configuration for the connections. */
    private final MongoDbConfiguration myConfig;

    /** The factory for creating connections to the servers. */
    private final ProxiedConnectionFactory myConnectionFactory;

    /** The thread that is pinging the servers for latency. */
    private final Thread myPingThread;

    /**
     * Creates a new ConnectionPinger.
     * 
     * @param cluster
     *            The state of the cluster.
     * @param factory
     *            The factory for creating connections to the servers.
     * @param config
     *            The configuration for the connections.
     */
    public ConnectionPinger(final ClusterState cluster,
            final ProxiedConnectionFactory factory,
            final MongoDbConfiguration config) {
        super();

        myCluster = cluster;
        myConnectionFactory = factory;
        myConfig = config;

        myPingThread = myConfig.getThreadFactory().newThread(this);
        myPingThread.setDaemon(true);
        myPingThread.setName("MongoDB Pinger");
        myPingThread.setPriority(Thread.MIN_PRIORITY);
        myPingThread.start();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the pinger.
     * </p>
     */
    @Override
    public void close() throws IOException {
        myPingThread.interrupt();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to periodically wake-up and ping the servers. At first this
     * will occur fairly often but eventually degrade two once every 5 minutes.
     * </p>
     */
    @Override
    public void run() {
        try {
            int sleepSeconds = MIN_PING_INTERVAL_SECONDS;
            while (true) {
                for (final ServerState server : myCluster.getServers()) {
                    final InetSocketAddress addr = server.getServer();
                    Connection conn = null;
                    try {
                        myPingThread.setName("MongoDB Pinger - " + addr);

                        conn = myConnectionFactory.connect(addr, myConfig);

                        final long start = System.nanoTime();

                        // Use a server status request to measure latency. It is
                        // a best case since it does not require any locks.
                        if (ping(addr, conn)) {
                            final long end = System.nanoTime();

                            server.updateAverageLatency(TimeUnit.NANOSECONDS
                                    .toMillis(end - start));
                        }
                    }
                    catch (final IOException e) {
                        LOG.log(Level.INFO, "Could not ping '" + addr + "'.", e);
                    }
                    finally {
                        myPingThread.setName("MongoDB Pinger - Idle");
                        IOUtils.close(conn);
                    }
                }

                // Sleep a little between sweeps.
                Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
                sleepSeconds = Math.min(MAX_PING_INTERVAL_SECONDS, sleepSeconds
                        + PING_INTERVAL_INCREMENT_SECONDS);
            }
        }
        catch (final InterruptedException ok) {
            LOG.info("Closing pinger on interrupt.");
        }
    }

    /**
     * Pings the server an suppresses all exceptions.
     * 
     * @param addr
     *            The address of the server. Used for logging.
     * @param conn
     *            The connection to ping.
     * @return True if the ping worked, false otherwise.
     */
    public static boolean ping(InetSocketAddress addr, Connection conn) {
        try {
            final FutureCallback<Reply> future = new FutureCallback<Reply>();
            conn.send(future, new ServerStatus());
            future.get(10, TimeUnit.MINUTES);

            return true;
        }
        catch (final MongoDbException e) {
            LOG.log(Level.INFO, "Could not ping '" + addr + "'.", e);
        }
        catch (final ExecutionException e) {
            LOG.log(Level.INFO, "Could not ping '" + addr + "'.", e);
        }
        catch (final TimeoutException e) {
            LOG.log(Level.INFO, "'" + addr
                    + "' might be a zombie - not receiving "
                    + "a response to ping.", e);
        }
        catch (InterruptedException e) {
            LOG.log(Level.INFO, "Interrupted pinging '" + addr + "'.", e);
        }

        return false;
    }
}
