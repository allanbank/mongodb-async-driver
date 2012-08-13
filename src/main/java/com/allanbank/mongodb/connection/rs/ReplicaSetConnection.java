/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
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
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
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
            final MongoDbConfiguration config) {
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
    public String send(final Callback<Reply> reply, final Message... messages)
            throws MongoDbException {

        final List<ServerState> servers = findPotentialServers(messages);

        // First we try and send to a server with a connection already open.
        String result = trySendToOpenConnection(servers, reply, messages);
        if (result == null) {
            // Just get it out the door.
            result = trySend(servers, reply, messages);
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
     * Locates the set of servers that can be used to send the specified
     * messages.
     * 
     * @param messages
     *            The messages to be sent.
     * @return The servers that can be used.
     * @throws MongoDbException
     *             On a failure to locate a server that all messages can be sent
     *             to.
     */
    protected List<ServerState> findPotentialServers(final Message... messages)
            throws MongoDbException {
        List<ServerState> servers;
        if (0 < messages.length) {
            List<ServerState> potentialServers = myCluster
                    .findCandidateServers(messages[0].getReadPreference());
            servers = potentialServers;

            if (1 < messages.length) {
                servers = new ArrayList<ServerState>(potentialServers);
                for (int i = 1; i < messages.length; ++i) {
                    potentialServers = myCluster
                            .findCandidateServers(messages[i]
                                    .getReadPreference());
                    servers.retainAll(potentialServers);
                }
            }

            if (servers.isEmpty()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Could not find any servers for the following set of read preferences: ");
                final Set<ReadPreference> seen = new HashSet<ReadPreference>();
                for (final Message message : messages) {
                    final ReadPreference prefs = message.getReadPreference();
                    if (seen.add(prefs)) {
                        if (seen.size() == 1) {
                            builder.append(", ");
                        }
                        builder.append(prefs);
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
     * Tries to send the messages to the first server with either an open
     * connection or that we can open a connection to.
     * 
     * @param servers
     *            The servers the messages can be sent to.
     * @param reply
     *            The callback for the replies.
     * @param messages
     *            The messages to send.
     * @return The token for the server that the messages were sent to or
     *         <code>null</code> if the messages could not be sent.
     */
    protected String trySend(final List<ServerState> servers,
            final Callback<Reply> reply, final Message... messages) {
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
                    // Oops. Closed while we were not looking.
                    // Do a reconnect.
                    final ReconnectStrategy strategy = myFactory
                            .getReconnectStrategy();
                    final Connection newConn = strategy.reconnect(conn);
                    IOUtils.close(conn);
                    conn = newConn;
                }

                if (conn != null) {
                    return conn.send(reply, messages);
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
     * @param reply
     *            The callback for the replies.
     * @param messages
     *            The messages to send.
     * @return The token for the server that the messages were sent to or
     *         <code>null</code> if the messages could not be sent.
     */
    protected String trySendToOpenConnection(final List<ServerState> servers,
            final Callback<Reply> reply, final Message... messages) {
        for (final ServerState server : servers) {

            // Check if sending to the primary.
            if (server.equals(myPrimaryServer)) {
                return super.send(reply, messages);
            }

            Connection conn = null;
            try {
                conn = server.takeConnection();

                if ((conn != null) && !conn.isOpen()) {
                    // Oops. Closed while we were not looking.
                    // Do a reconnect.
                    final ReconnectStrategy strategy = myFactory
                            .getReconnectStrategy();
                    final Connection newConn = strategy.reconnect(conn);
                    IOUtils.close(conn);
                    conn = newConn;
                }

                if (conn != null) {
                    return conn.send(reply, messages);
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
