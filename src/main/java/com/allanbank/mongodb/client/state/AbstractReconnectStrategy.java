/*
 * #%L
 * AbstractReconnectStrategy.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.state;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;

/**
 * AbstractReconnectStrategy provides a base class for reconnection strategies.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractReconnectStrategy implements ReconnectStrategy {

    /** The configuration for connections to the servers. */
    protected MongoClientConfiguration myConfig = null;

    /** The connection factory for new connections. */
    protected ProxiedConnectionFactory myConnectionFactory = null;

    /** The selector for which server to connect to. */
    protected ServerSelector mySelector = null;

    /** The state of the cluster. */
    protected Cluster myState = null;

    /**
     * Creates a new AbstractReconnectStrategy.
     */
    public AbstractReconnectStrategy() {
        super();
    }

    /**
     * Returns the configuration for connections to the servers.
     * 
     * @return The configuration for connections to the servers
     */
    public MongoClientConfiguration getConfig() {
        return myConfig;
    }

    /**
     * Returns the connection factory for new connections.
     * 
     * @return The connection factory for new connections.
     */
    public ProxiedConnectionFactory getConnectionFactory() {
        return myConnectionFactory;
    }

    /**
     * Returns the selector for which server to connect to.
     * 
     * @return The selector for which server to connect to.
     */
    public ServerSelector getSelector() {
        return mySelector;
    }

    /**
     * Returns the state of the cluster.
     * 
     * @return The state of the cluster.
     */
    public Cluster getState() {
        return myState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(final MongoClientConfiguration config) {
        myConfig = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConnectionFactory(
            final ProxiedConnectionFactory connectionFactory) {
        myConnectionFactory = connectionFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSelector(final ServerSelector selector) {
        mySelector = selector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setState(final Cluster state) {
        myState = state;
    }

    /**
     * Pings the server to verify that the connection is active.
     * 
     * @param server
     *            The server being connected to.
     * @param connection
     *            The connection to verify.
     * @return True if the connection is working, false otherwise.
     */
    protected boolean isConnected(final Server server,
            final Connection connection) {
        return ClusterPinger.ping(server, connection);
    }

}