/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.util.IOUtils;

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

    /** The configuration for the connections. */
    private final MongoDbConfiguration myConfig;

    /** The factory for creating connections to the servers. */
    private final ProxiedConnectionFactory myConnectionFactory;

    /** The thread that is pinging the servers for latency. */
    private final Thread myPingThread;

    /** The flag to stop the ping thread. */
    private volatile boolean myRunning;

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
        myRunning = true;

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
        myRunning = false;
        myPingThread.interrupt();
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
        try {
            int sleepSeconds = MIN_PING_INTERVAL_SECONDS;
            while (myRunning) {
                for (final ServerState server : myCluster.getServers()) {
                    final InetSocketAddress addr = server.getServer();
                    Connection conn = null;
                    try {
                        myPingThread.setName("MongoDB Pinger - " + addr);

                        conn = myConnectionFactory.connect(server, myConfig);

                        final long start = System.nanoTime();

                        // Use a server status request to measure latency. It is
                        // a best case since it does not require any locks.
                        if (PINGER.ping(addr, conn, server)) {
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
     * Pinger provides logic to ping servers.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class Pinger {
        /**
         * Pings the server and suppresses all exceptions. Updates the server
         * state with the tags found in the response, if any.
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
                final FutureCallback<Reply> future = new FutureCallback<Reply>();
                conn.send(future, new IsMaster());
                final Reply reply = future.get(10, TimeUnit.MINUTES);

                if (state != null) {
                    state.setTags(extractTags(reply));
                }

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
            catch (final InterruptedException e) {
                LOG.log(Level.INFO, "Interrupted pinging '" + addr + "'.", e);
            }

            return false;
        }

        /**
         * Extract any tags from the ping reply.
         * 
         * @param reply
         *            The reply.
         * @return The tags document, which may be <code>null</code>.
         */
        private Document extractTags(final Reply reply) {
            Document result = null;
            final List<Document> replyDocs = reply.getResults();
            if (replyDocs.size() >= 1) {
                final Document doc = replyDocs.get(0);
                final List<DocumentElement> tags = doc.queryPath(
                        DocumentElement.class, "tags");
                if (!tags.isEmpty()) {
                    result = tags.get(0).asDocument();
                }
            }
            return result;
        }
    }
}
