/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.sharded;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyMultipleConnection;
import com.allanbank.mongodb.client.connection.proxy.ConnectionInfo;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerSelector;

/**
 * Provides a {@link Connection} implementation for connecting to a sharded
 * environment via mongos servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnection extends AbstractProxyMultipleConnection<Server> {

    /** The selector for the server when we need to reconnect. */
    private final ServerSelector mySelector;

    /**
     * Creates a new {@link ShardedConnection}.
     * 
     * @param proxiedConnection
     *            The connection being proxied.
     * @param server
     *            The primary server this connection is connected to.
     * @param cluster
     *            The state of the cluster for finding secondary connections.
     * @param selector
     *            The selector for servers when we need to reconnect.
     * @param factory
     *            The connection factory for opening secondary connections.
     * @param config
     *            The MongoDB client configuration.
     */
    public ShardedConnection(final Connection proxiedConnection,
            final Server server, final Cluster cluster,
            final ServerSelector selector,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        super(proxiedConnection, server, cluster, factory, config);

        mySelector = selector;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the canonical name of the primary.
     * </p>
     */
    @Override
    public String getServerName() {
        if (myMainKey != null) {
            return myMainKey.getCanonicalName();
        }
        return "UNKNOWN";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "Sharded(" + myLastUsedConnection.get() + ")";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a connection to the server.
     * </p>
     */
    @Override
    protected Connection connect(final Server server) {
        Connection conn = null;
        try {
            conn = myFactory.connect(server, myConfig);

            conn = cacheConnection(server, conn);
        }
        catch (final IOException e) {
            LOG.info("Could not connect to the server '"
                    + server.getCanonicalName() + "': " + e.getMessage());
        }
        return conn;
    }

    /**
     * Locates the set of servers that can be used to send the specified
     * messages. This method will attempt to connect to the primary server if
     * there is not a current connection to the primary.
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
    @Override
    protected List<Server> findPotentialKeys(final Message message1,
            final Message message2) throws MongoDbException {
        List<Server> servers = doFindPotentialServers(message1, message2);

        if (servers.isEmpty()) {
            // If we get here and a reconnect is in progress then
            // block for the reconnect. Once the reconnect is complete, try
            // again.
            if (myMainKey == null) {
                // Wait for a reconnect.
                final ConnectionInfo<Server> newConnInfo = reconnectMain();
                if (newConnInfo != null) {
                    updateMain(newConnInfo);
                    servers = doFindPotentialServers(message1, message2);
                }
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
                            if (seen.size() > 1) {
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

        return servers;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden creates a connection back to the primary server.
     * </p>
     */
    @Override
    protected ConnectionInfo<Server> reconnectMain() {
        for (final Server server : mySelector.pickServers()) {
            try {
                final Connection conn = myFactory.connect(server, myConfig);

                return new ConnectionInfo<Server>(conn, server);
            }
            catch (final IOException ioe) {
                // Ignored. Will return null.
                LOG.fine("Could not connect to '" + server + "': "
                        + ioe.getMessage());
            }
        }
        return null;
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
     */
    private List<Server> doFindPotentialServers(final Message message1,
            final Message message2) {

        final Server main = myMainKey;
        List<Server> servers = Collections.emptyList();
        if (message1 != null) {
            ReadPreference pref = message1.getReadPreference();
            if (pref.getServer() != null) {
                servers = Collections.singletonList(myCluster.get(pref
                        .getServer()));
            }
            else {
                pref = message2.getReadPreference();
                if (pref.getServer() != null) {
                    servers = Collections.singletonList(myCluster.get(pref
                            .getServer()));
                }
                else if (main != null) {
                    servers = Collections.singletonList(main);
                }
            }
        }
        return servers;
    }
}
