/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.state.AbstractReconnectStrategy;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetReconnectStrategy extends
        AbstractReconnectStrategy<ReplicaSetConnection> {

    /**
     * The initial amount of time to pause waiting for a server to take over as
     * the primary.
     */
    public static final int INITIAL_RECONNECT_PAUSE_TIME_MS = 100;

    /**
     * The Maximum amount of time to pause waiting for a server to take over as
     * the primary.
     */
    public static final int MAX_RECONNECT_PAUSE_TIME_MS = 5000;

    /** The logger for the {@link ReplicaSetReconnectStrategy}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetReconnectStrategy.class.getCanonicalName());

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
     * {@link MongoDbConfiguration#getReconnectTimeout()} has expired.
     * </p>
     */
    @Override
    public ReplicaSetConnection reconnect(
            final ReplicaSetConnection oldConnection) {

        LOG.fine("Trying replica set reconnect.");

        final ClusterState state = getState();

        final Map<InetSocketAddress, Future<Reply>> answers = new HashMap<InetSocketAddress, Future<Reply>>();
        final Map<InetSocketAddress, Connection> connections = new HashMap<InetSocketAddress, Connection>();
        try {
            // Figure out a deadline for the reconnect.
            final int wait = getConfig().getReconnectTimeout();
            long now = System.currentTimeMillis();
            final long deadline = (wait <= 0) ? Long.MAX_VALUE : (now + wait);

            // How much time to pause for replies and waiting for a server to
            // become primary.
            int pauseTime = INITIAL_RECONNECT_PAUSE_TIME_MS;
            while (now < deadline) {
                // Ask all of the servers who they think the primary is.
                for (final ServerState server : state.getServers()) {

                    sendIsPrimary(answers, connections, server, false);

                    // Anyone replied yet?
                    final ReplicaSetConnection newConn = checkForReply(
                            oldConnection, answers, connections);
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
                final ReplicaSetConnection newConn = checkForReply(
                        oldConnection, answers, connections);
                if (newConn != null) {
                    return newConn;
                }

                now = System.currentTimeMillis();
            }
        }
        finally {
            // Shut down the connections we created.
            for (final Connection conn : connections.values()) {
                IOUtils.close(conn);
            }
        }
        return null;
    }

    /**
     * Checks for a reply from a server. If one has been received then it tries
     * to confirm the primary server by asking it if it thinks it is the primary
     * server.
     * 
     * @param oldConnection
     *            The old connection to copy from.
     * @param answers
     *            The pending ({@link Future}) answers from each server.
     * @param connections
     *            The connection to each server.
     * @return The new connection if there was a reply and that server confirmed
     *         it was the primary.
     */
    protected ReplicaSetConnection checkForReply(
            final ReplicaSetConnection oldConnection,
            final Map<InetSocketAddress, Future<Reply>> answers,
            final Map<InetSocketAddress, Connection> connections) {
        final Map<InetSocketAddress, Future<Reply>> copy = new HashMap<InetSocketAddress, Future<Reply>>(
                answers);
        for (final Map.Entry<InetSocketAddress, Future<Reply>> entry : copy
                .entrySet()) {

            final InetSocketAddress addr = entry.getKey();
            final Future<Reply> reply = entry.getValue();

            if (reply.isDone()) {
                // Remove this reply.
                answers.remove(addr);

                // Check the result.
                final String putativePrimary = checkReply(reply, connections,
                        addr);

                // Phase2 - Verify the putative server.
                if ((putativePrimary != null)
                        && verifyPutative(answers, connections, putativePrimary)) {

                    // Phase 3 - Setup a new replica set connection to the
                    // primary and seed it with a secondary if there is a
                    // suitable server.
                    final ServerState server = getState().get(putativePrimary);

                    // Mark the server writable.
                    // There can only be 1 writable server.
                    for (final ServerState other : getState()
                            .getWritableServers()) {
                        getState().markNotWritable(other);
                    }
                    getState().markWritable(server);

                    final Connection primaryConn = connections.remove(server
                            .getServer());
                    final ReplicaSetConnection newRsConn = new ReplicaSetConnection(
                            primaryConn, getConfig());

                    // See if we have a suitable secondary server connection.
                    final List<ServerState> servers = getSelector()
                            .pickServers();
                    for (final ServerState secondaryServer : servers) {
                        final Connection secondaryConn = connections
                                .remove(secondaryServer.getServer());
                        if (secondaryConn != null) {
                            newRsConn.setSecondaryConnection(secondaryConn);
                            break;
                        }
                    }

                    // Copy the pending messages.
                    copyPending(newRsConn, oldConnection);

                    return newRsConn;
                }
            }
            else {
                LOG.fine("No reply yet from " + addr);
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
     * @param addr
     *            The address of the server.
     * @return The name of the server the reply indicates is the primary, null
     *         if there is no primary or any error.
     */
    protected String checkReply(final Future<Reply> replyFuture,
            final Map<InetSocketAddress, Connection> connections,
            final InetSocketAddress addr) {
        if (replyFuture != null) {
            try {
                final Reply reply = replyFuture.get();
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
            catch (final ExecutionException e) {
                // Kill the associated connection.
                final Connection conn = connections.remove(addr);
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
            final Map<InetSocketAddress, Future<Reply>> answers,
            final Map<InetSocketAddress, Connection> connections,
            final ServerState server, final boolean isPrimary) {
        Future<Reply> reply = null;
        final InetSocketAddress addr = server.getServer();
        try {
            // Locate a connection to the server.
            Connection conn = connections.get(addr);
            if ((conn == null) || (conn.isOpen() == false)) {
                conn = getConnectionFactory().connect(addr, getConfig());
                connections.put(addr, conn);
            }

            // Only send to the server if there is not an outstanding
            // request.
            reply = answers.get(addr);
            if (reply == null) {
                LOG.fine("Sending reconnect(rs) query to " + server.getServer());

                final FutureCallback<Reply> replyCallback = new FutureCallback<Reply>();
                conn.send(replyCallback, new IsMaster());

                reply = replyCallback;
                answers.put(addr, reply);
            }
        }
        catch (final IOException e) {
            // Nothing to do for now. Log at a debug level if this is not the
            // primary. Warn if we think it is the primary.
            final Level level = isPrimary ? Level.WARNING : Level.FINE;
            LOG.log(level, "Cannot create a connection to '" + addr + "'.", e);
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
     * @return True if the server concurs that it is the primary.
     */
    protected boolean verifyPutative(
            final Map<InetSocketAddress, Future<Reply>> answers,
            final Map<InetSocketAddress, Connection> connections,
            final String putativePrimary) {

        LOG.fine("Verify putative server (" + putativePrimary
                + ") on reconnect(rs).");

        final ServerState server = getState().get(putativePrimary);

        // Make sure we send a new request. The old reply might have been
        // before becoming the primary.
        answers.remove(server.getServer());

        // If the primary agrees that they are the primary then it is
        // probably true.
        final Future<Reply> reply = sendIsPrimary(answers, connections, server,
                true);
        final String primary = checkReply(reply, connections,
                server.getServer());
        if (putativePrimary.equals(primary)) {
            LOG.info("New primary for replica set: " + putativePrimary);
            return true;
        }

        return false;
    }
}
