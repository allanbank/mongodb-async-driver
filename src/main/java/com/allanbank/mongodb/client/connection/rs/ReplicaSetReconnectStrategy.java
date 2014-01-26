/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ConnectionInfo;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.state.AbstractReconnectStrategy;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerUpdateCallback;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ReplicaSetReconnectStrategy provides a {@link ReconnectStrategy} designed for
 * replica sets. The reconnect strategy attempts to locate the primary member of
 * the replica set by:
 * <ol>
 * <li>Querying each member of the replica set for the primary server.</li>
 * <li>Once a primary server has been identified by a member of the replica set
 * (the putative primary) the putative primary server is queried for the primary
 * server.</li>
 * <ol>
 * <li>If the putative primary concurs that it is the primary then the search
 * completes and the primary server's connection is used.</li>
 * <li>If the putative primary does not concur then the search continues
 * scanning each server in turn for the primary server.</li>
 * </ol>
 * </ol>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetReconnectStrategy extends AbstractReconnectStrategy {

    /**
     * The initial amount of time to pause waiting for a server to take over as
     * the primary.
     */
    public static final int INITIAL_RECONNECT_PAUSE_TIME_MS = 10;

    /**
     * The Maximum amount of time to pause waiting for a server to take over as
     * the primary.
     */
    public static final int MAX_RECONNECT_PAUSE_TIME_MS = 1000;

    /** The logger for the {@link ReplicaSetReconnectStrategy}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetReconnectStrategy.class.getCanonicalName());

    /** The set of servers we cannot connect to. */
    private final Set<Server> myDeadServers = Collections
            .newSetFromMap(new ConcurrentHashMap<Server, Boolean>());

    /**
     * Creates a new ReplicaSetReconnectStrategy.
     */
    public ReplicaSetReconnectStrategy() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to search for the primary server in the replica set. This will
     * only continue until the
     * {@link MongoClientConfiguration#getReconnectTimeout()} has expired.
     * </p>
     */
    @Override
    public ReplicaSetConnection reconnect(final Connection oldConnection) {
        final ConnectionInfo<Server> info = reconnectPrimary();
        if (info != null) {
            return new ReplicaSetConnection(info.getConnection(),
                    info.getConnectionKey(), getState(),
                    getConnectionFactory(), getConfig(), this);
        }
        return null;
    }

    /**
     * Overridden to search for the primary server in the replica set. This will
     * only continue until the
     * {@link MongoClientConfiguration#getReconnectTimeout()} has expired. </p>
     * 
     * @return The information for the primary connection or null if the
     *         reconnect fails.
     */
    public synchronized ConnectionInfo<Server> reconnectPrimary() {
        LOG.fine("Trying replica set reconnect.");
        final Cluster state = getState();

        // Figure out a deadline for the reconnect.
        final int wait = getConfig().getReconnectTimeout();
        long now = System.currentTimeMillis();
        final long deadline = (wait <= 0) ? Long.MAX_VALUE : (now + wait);

        final Map<Server, Future<Reply>> answers = new HashMap<Server, Future<Reply>>();
        final Map<Server, Connection> connections = new HashMap<Server, Connection>();

        // Clear any interrupts
        final boolean interrupted = Thread.interrupted();
        try {
            // First try a simple reconnect.
            for (final Server writable : state.getWritableServers()) {
                if (verifyPutative(answers, connections, writable, deadline)) {
                    LOG.fine("New primary for replica set: "
                            + writable.getCanonicalName());
                    return createReplicaSetConnection(connections, writable);
                }
            }

            // How much time to pause for replies and waiting for a server
            // to become primary.
            int pauseTime = INITIAL_RECONNECT_PAUSE_TIME_MS;
            while (now < deadline) {
                // Ask all of the servers who they think the primary is.
                for (final Server server : state.getServers()) {

                    sendIsPrimary(answers, connections, server, false);

                    // Anyone replied yet?
                    final ConnectionInfo<Server> newConn = checkForReply(state,
                            answers, connections, deadline);
                    if (newConn != null) {
                        return newConn;
                    }

                    // Loop to the next server.
                }

                // Wait for a beat for a reply or a server to decide to be
                // master.
                sleep(pauseTime, MILLISECONDS);
                pauseTime = Math.min(MAX_RECONNECT_PAUSE_TIME_MS, pauseTime
                        + pauseTime);

                // Check again for replies before trying to reconnect.
                final ConnectionInfo<Server> newConn = checkForReply(state,
                        answers, connections, deadline);
                if (newConn != null) {
                    return newConn;
                }

                now = System.currentTimeMillis();
            }
        }
        finally {
            // Shut down the connections we created.
            for (final Connection conn : connections.values()) {
                conn.shutdown(true);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     * Checks for a reply from a server. If one has been received then it tries
     * to confirm the primary server by asking it if it thinks it is the primary
     * server.
     * 
     * @param state
     *            The state of the cluster.
     * @param answers
     *            The pending ({@link Future}) answers from each server.
     * @param connections
     *            The connection to each server.
     * @param deadline
     *            The deadline for the reconnect attempt.
     * @return The new connection if there was a reply and that server confirmed
     *         it was the primary.
     */
    protected ConnectionInfo<Server> checkForReply(final Cluster state,
            final Map<Server, Future<Reply>> answers,
            final Map<Server, Connection> connections, final long deadline) {
        final Map<Server, Future<Reply>> copy = new HashMap<Server, Future<Reply>>(
                answers);
        for (final Map.Entry<Server, Future<Reply>> entry : copy.entrySet()) {

            final Server server = entry.getKey();
            final Future<Reply> reply = entry.getValue();

            if (reply.isDone()) {
                // Remove this reply.
                answers.remove(server);

                // Check the result.
                final String putative = checkReply(reply, connections, server,
                        deadline);

                // Phase2 - Verify the putative server.
                if (putative != null) {
                    final Server putativeServer = getState().get(putative);
                    if (verifyPutative(answers, connections, putativeServer,
                            deadline)) {

                        // Phase 3 - Setup a new replica set connection to the
                        // primary and seed it with a secondary if there is a
                        // suitable server.
                        LOG.info("New primary for replica set: " + putative);
                        updateUnknown(state, answers, connections);
                        return createReplicaSetConnection(connections,
                                putativeServer);
                    }
                }
            }
            else {
                LOG.fine("No reply yet from " + server);
            }
        }

        return null;
    }

    /**
     * Extracts who the server thinks is the primary from the reply.
     * 
     * @param replyFuture
     *            The future to get the reply from.
     * @param connections
     *            The map of connections. The connection will be closed on an
     *            error.
     * @param server
     *            The server.
     * @param deadline
     *            The deadline for the reconnect attempt.
     * @return The name of the server the reply indicates is the primary, null
     *         if there is no primary or any error.
     */
    protected String checkReply(final Future<Reply> replyFuture,
            final Map<Server, Connection> connections, final Server server,
            final long deadline) {
        if (replyFuture != null) {
            try {
                final Reply reply = replyFuture.get(
                        Math.max(0, deadline - System.currentTimeMillis()),
                        TimeUnit.MILLISECONDS);

                final List<Document> results = reply.getResults();
                if (!results.isEmpty()) {
                    final Document doc = results.get(0);

                    // Get the name of the primary server.
                    final Element primary = doc.get("primary");
                    if (primary instanceof StringElement) {
                        return ((StringElement) primary).getValue();
                    }
                }
            }
            catch (final InterruptedException e) {
                // Just ignore the reply.
            }
            catch (final TimeoutException e) {
                // Kill the associated connection.
                final Connection conn = connections.remove(server);
                IOUtils.close(conn);
            }
            catch (final ExecutionException e) {
                // Kill the associated connection.
                final Connection conn = connections.remove(server);
                IOUtils.close(conn);
            }
        }
        return null;
    }

    /**
     * Sends a command to the server to return what it thinks the state of the
     * cluster is. This method will not re-request the information from the
     * server if there is already an outstanding request.
     * 
     * @param answers
     *            The pending ({@link Future}) answers from each server.
     * @param connections
     *            The connection to each server.
     * @param server
     *            The server to send the request to.
     * @param isPrimary
     *            If true logs connection errors as warnings. Debug otherwise.
     * @return The future reply for the request sent to the server.
     */
    protected Future<Reply> sendIsPrimary(
            final Map<Server, Future<Reply>> answers,
            final Map<Server, Connection> connections, final Server server,
            final boolean isPrimary) {
        Future<Reply> reply = null;
        try {
            // Locate a connection to the server.
            Connection conn = connections.get(server);
            if ((conn == null) || !conn.isAvailable()) {
                conn = getConnectionFactory().connect(server, getConfig());
                connections.put(server, conn);
            }

            // Only send to the server if there is not an outstanding
            // request.
            reply = answers.get(server);
            if (reply == null) {
                LOG.fine("Sending reconnect(rs) query to "
                        + server.getCanonicalName());

                final FutureCallback<Reply> replyCallback = new ServerUpdateCallback(
                        server);
                conn.send(new IsMaster(), replyCallback);

                reply = replyCallback;
                answers.put(server, reply);

                myDeadServers.remove(server);
            }
        }
        catch (final IOException e) {
            // Nothing to do for now. Log at a debug level if this is not the
            // primary. Warn if we think it is the primary (and have not warned
            // before)
            final Level level = (isPrimary && myDeadServers.add(server)) ? Level.WARNING
                    : Level.FINE;
            LOG.log(level, "Cannot create a connection to '" + server + "'.", e);
        }

        return reply;
    }

    /**
     * Sleeps without throwing an exception.
     * 
     * @param sleepTime
     *            The amount of time to sleep.
     * @param units
     *            The untis for the amount of time to sleep.
     */
    protected void sleep(final int sleepTime, final TimeUnit units) {
        try {
            units.sleep(sleepTime);
        }
        catch (final InterruptedException e) {
            // Ignore.
        }
    }

    /**
     * Tries to verify that the suspected primary server is in fact the primary
     * server by asking it directly and synchronously.
     * 
     * @param answers
     *            The pending ({@link Future}) answers from each server.
     * @param connections
     *            The connection to each server.
     * @param putativePrimary
     *            The server we think is the primary.
     * @param deadline
     *            The deadline for the reconnect attempt.
     * @return True if the server concurs that it is the primary.
     */
    protected boolean verifyPutative(final Map<Server, Future<Reply>> answers,
            final Map<Server, Connection> connections,
            final Server putativePrimary, final long deadline) {

        LOG.fine("Verify putative server (" + putativePrimary
                + ") on reconnect(rs).");

        // Make sure we send a new request. The old reply might have been
        // before becoming the primary.
        answers.remove(putativePrimary);

        // If the primary agrees that they are the primary then it is
        // probably true.
        final Future<Reply> reply = sendIsPrimary(answers, connections,
                putativePrimary, true);
        final String primary = checkReply(reply, connections, putativePrimary,
                deadline);
        if (putativePrimary.getCanonicalName().equals(primary)) {
            return true;
        }

        return false;
    }

    /**
     * Creates the {@link ReplicaSetConnection} for the primary server.
     * 
     * @param connections
     *            The connection that are being managed.
     * @param primaryServer
     *            The primary server.
     * @return The {@link ReplicaSetConnection}.
     */
    private ConnectionInfo<Server> createReplicaSetConnection(
            final Map<Server, Connection> connections,
            final Server primaryServer) {
        final Connection primaryConn = connections.remove(primaryServer);

        return new ConnectionInfo<Server>(primaryConn, primaryServer);
    }

    /**
     * Tries to send messages to all of the members of the cluster in an
     * indeterminate state.
     * 
     * @param state
     *            The state of the cluster.
     * @param answers
     *            The pending responses.
     * @param connections
     *            The connection already created.
     */
    private void updateUnknown(final Cluster state,
            final Map<Server, Future<Reply>> answers,
            final Map<Server, Connection> connections) {
        for (final Server server : state.getServers()) {
            switch (server.getState()) {
            case UNKNOWN: // Fall through.
            case UNAVAILABLE: {
                answers.remove(server);
                sendIsPrimary(answers, connections, server, false);
                break;
            }
            case READ_ONLY:
            case WRITABLE:
            default: {
                // Known good.
                break;
            }
            }
        }
    }
}
