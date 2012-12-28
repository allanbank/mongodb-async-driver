/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides a {@link Connection} implementation for connecting to a replica-set
 * environment.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnection extends AbstractProxyConnection {

    /** The logger for the {@link ReplicaSetConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetConnection.class.getCanonicalName());

    /** The state of the cluster for finding secondary connections. */
    private final ClusterState myCluster;

    /** The connection factory for opening secondary connections. */
    private final ProxiedConnectionFactory myFactory;

    /** The primary server this connection is connected to. */
    private final ServerState myPrimaryServer;

    /**
     * Creates a new {@link ReplicaSetConnection}.
     * 
     * @param proxiedConnection
     *            The connection being proxied.
     * @param server
     *            The primary server this connection is connected to.
     * @param cluster
     *            The state of the cluster for finding secondary connections.
     * @param factory
     *            The connection factory for opening secondary connections.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnection(final Connection proxiedConnection,
            final ServerState server, final ClusterState cluster,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        super(proxiedConnection, config);
        myPrimaryServer = server;
        myCluster = cluster;
        myFactory = factory;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Locates all of the potential servers that can receive all of the
     * messages. Tries to then send the messages to a server with a connection
     * already open or failing that tries to open a connection to open of the
     * servers.
     * </p>
     */
    @Override
    public String send(final Message message,
            final Callback<Reply> replyCallback) throws MongoDbException {
        return send(message, null, replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Locates all of the potential servers that can receive all of the
     * messages. Tries to then send the messages to a server with a connection
     * already open or failing that tries to open a connection to open of the
     * servers.
     * </p>
     */
    @Override
    public String send(final Message message1, final Message message2,
            final Callback<Reply> replyCallback) throws MongoDbException {
        final List<ServerState> servers = findPotentialServers(message1,
                message2);

        // First we try and send to a server with a connection already open.
        String result = trySendToOpenConnection(servers, message1, message2,
                replyCallback);
        if (result == null) {
            // Just get it out the door.
            result = trySend(servers, message1, message2, replyCallback);
        }

        if (result == null) {
            throw new MongoDbException(
                    "Could not send the messages to any of the potential servers.");
        }

        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "ReplicaSet(" + getProxiedConnection() + ")";
    }

    /**
     * Sends the message on the connection.
     * 
     * @param conn
     *            The connection to send on.
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send, may be <code>null</code>.
     * @param reply
     *            The reply {@link Callback}.
     * @return The server the message was sent to.
     */
    protected String doSend(final Connection conn, final Message message1,
            final Message message2, final Callback<Reply> reply) {
        if (message2 == null) {
            return conn.send(message1, reply);
        }
        return conn.send(message1, message2, reply);

    }

    /**
     * Locates the set of servers that can be used to send the specified
     * messages.
     * 
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send. May be <code>null</code>.
     * @return The servers that can be used.
     * @throws MongoDbException
     *             On a failure to locate a server that all messages can be sent
     *             to.
     */
    protected List<ServerState> findPotentialServers(final Message message1,
            final Message message2) throws MongoDbException {
        List<ServerState> servers;
        if (message1 != null) {
            List<ServerState> potentialServers = myCluster
                    .findCandidateServers(message1.getReadPreference());
            servers = potentialServers;

            if (message2 != null) {
                servers = new ArrayList<ServerState>(potentialServers);
                potentialServers = myCluster.findCandidateServers(message2
                        .getReadPreference());
                servers.retainAll(potentialServers);
            }

            if (servers.isEmpty()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Could not find any servers for the following set of read preferences: ");
                final Set<ReadPreference> seen = new HashSet<ReadPreference>();
                for (final Message message : Arrays.asList(message1, message2)) {
                    if (message != null) {
                        final ReadPreference prefs = message
                                .getReadPreference();
                        if (seen.add(prefs)) {
                            if (seen.size() == 1) {
                                builder.append(", ");
                            }
                            builder.append(prefs);
                        }
                    }
                }

                builder.append('.');

                throw new MongoDbException(builder.toString());
            }
        }
        else {
            servers = Collections.singletonList(myPrimaryServer);
        }
        return servers;
    }

    /**
     * Reconnects the connection.
     * 
     * @param conn
     *            The connection to reconnect.
     * @return The new connection if the reconnect was successful.
     */
    protected Connection reconnect(final Connection conn) {
        final ReconnectStrategy strategy = myFactory.getReconnectStrategy();
        final Connection newConn = strategy.reconnect(conn);
        IOUtils.close(conn);
        return newConn;
    }

    /**
     * Tries to send the messages to the first server with either an open
     * connection or that we can open a connection to.
     * 
     * @param servers
     *            The servers the messages can be sent to.
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send. May be <code>null</code>.
     * @param reply
     *            The callback for the replies.
     * @return The token for the server that the messages were sent to or
     *         <code>null</code> if the messages could not be sent.
     */
    protected String trySend(final List<ServerState> servers,
            final Message message1, final Message message2,
            final Callback<Reply> reply) {
        for (final ServerState server : servers) {

            // No need to check for primary here. Already looked.

            Connection conn = null;
            try {
                conn = server.takeConnection();

                if (conn == null) {
                    // Create one.
                    try {
                        conn = myFactory.connect(server, myConfig);
                    }
                    catch (final IOException e) {
                        LOG.info("Could not connect to the server '"
                                + server.getName() + "': " + e.getMessage());
                    }
                }
                else if (!conn.isOpen()) {
                    final Connection newConn = reconnect(conn);
                    conn = newConn;
                }

                if (conn != null) {
                    return doSend(conn, message1, message2, reply);
                }
            }
            finally {
                if ((conn != null) && !server.addConnection(conn)) {
                    conn.shutdown();
                }
            }
        }

        return null;
    }

    /**
     * Tries to send the messages to the first server with an open connection.
     * 
     * @param servers
     *            The servers the messages can be sent to.
     * @param message1
     *            The first message to send.
     * @param message2
     *            The second message to send. May be <code>null</code>.
     * @param reply
     *            The callback for the replies.
     * @return The token for the server that the messages were sent to or
     *         <code>null</code> if the messages could not be sent.
     */
    protected String trySendToOpenConnection(final List<ServerState> servers,
            final Message message1, final Message message2,
            final Callback<Reply> reply) {
        for (final ServerState server : servers) {

            // Check if sending to the primary.
            if (server.equals(myPrimaryServer)) {
                if (message2 == null) {
                    return super.send(message1, reply);
                }
                return super.send(message1, message2, reply);
            }

            Connection conn = null;
            try {
                conn = server.takeConnection();

                if ((conn != null) && !conn.isOpen()) {
                    // Oops. Closed while we were not looking.
                    // Do a reconnect.
                    conn = reconnect(conn);
                }

                if (conn != null) {
                    return doSend(conn, message1, message2, reply);
                }
            }
            finally {
                if ((conn != null) && !server.addConnection(conn)) {
                    conn.shutdown();
                }
            }
        }

        return null;
    }
}
