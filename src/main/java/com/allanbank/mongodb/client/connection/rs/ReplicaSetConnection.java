/*
 * #%L
 * ReplicaSetConnection.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.rs;

import java.io.IOException;
import java.util.List;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyMultipleConnection;
import com.allanbank.mongodb.client.connection.proxy.ConnectionInfo;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Provides a {@link Connection} implementation for connecting to a replica-set
 * environment.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnection
        extends AbstractProxyMultipleConnection<Server> {

    /** The logger for the {@link ReplicaSetConnection}. */
    private static final Log LOG = LogFactory
            .getLog(ReplicaSetConnection.class);

    /** The strategy for reconnecting/finding the primary. */
    private volatile ReplicaSetReconnectStrategy myReconnectStrategy;

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
     * @param strategy
     *            The strategy for reconnecting/finding the primary.
     */
    public ReplicaSetConnection(final Connection proxiedConnection,
            final Server server, final Cluster cluster,
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config,
            final ReplicaSetReconnectStrategy strategy) {
        super(proxiedConnection, server, cluster, factory, config);

        myReconnectStrategy = strategy;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the canonical name of the primary.
     * </p>
     */
    @Override
    public Server getServer() {
        Server server = myMainKey;
        if (server != null) {
            return server;
        }
        return myCluster.getServers().get(0);
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
        List<Server> servers = myCluster.findServers(message1, message2);

        if (servers.isEmpty()) {
            // If we get here and a reconnect is in progress then
            // block for the reconnect. The primary will have been marked a
            // secondary... Once the reconnect is complete, try again.
            if (myMainKey == null) {
                // Wait for a reconnect.
                final ConnectionInfo<Server> newConnInfo = reconnectMain();
                if (newConnInfo != null) {
                    updateMain(newConnInfo);
                    servers = myCluster.findServers(message1, message2);
                }
            }

            if (servers.isEmpty()) {
                throw createReconnectFailure(message1, message2);
            }
        }

        return servers;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the string {@code ReplicaSet}.
     * </p>
     */
    @Override
    protected String getConnectionType() {
        return "ReplicaSet";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden creates a connection back to the primary server.
     * </p>
     */
    @Override
    protected ConnectionInfo<Server> reconnectMain() {
        return myReconnectStrategy.reconnectPrimary();
    }

}
