/*
 * #%L
 * AuthenticationConnectionFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.auth;

import java.io.IOException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.ClusterStats;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.metrics.MongoClientMetrics;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AuthenticationConnectionFactory wraps all of the connections with
 * {@link AuthenticatingConnection}s.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticationConnectionFactory
        implements ProxiedConnectionFactory {

    /** The default config. */
    private final MongoClientConfiguration myConfig;

    /** The connection factory to proxy connections to. */
    private final ProxiedConnectionFactory myProxiedConnectionFactory;

    /**
     * Creates a new AuthenticationConnectionFactory.
     *
     * @param factory
     *            The factory to wrap connections wit
     *            {@link AuthenticatingConnection}.
     * @param config
     *            The default config.
     */
    public AuthenticationConnectionFactory(
            final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        myProxiedConnectionFactory = factory;
        myConfig = config;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the proxied {@link ConnectionFactory}.
     * </p>
     */
    @Override
    public void close() {
        IOUtils.close(myProxiedConnectionFactory);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a connection then ensures the connection is properly
     * authenticated before reconnecting.
     * </p>
     */
    @Override
    public AuthenticatingConnection connect() throws IOException {
        return new AuthenticatingConnection(
                myProxiedConnectionFactory.connect(), myConfig);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to wrap the returned connection with an
     * {@link AuthenticatingConnection}.
     * </p>
     */
    @Override
    public AuthenticatingConnection connect(final Server server,
            final MongoClientConfiguration config) throws IOException {
        return new AuthenticatingConnection(myProxiedConnectionFactory.connect(
                server, config), config);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the cluster stats of the proxied
     * {@link ConnectionFactory}.
     * </p>
     */
    @Override
    public ClusterStats getClusterStats() {
        return myProxiedConnectionFactory.getClusterStats();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the cluster type of the proxied
     * {@link ConnectionFactory}.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return myProxiedConnectionFactory.getClusterType();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the metrics agent from the proxied connection factory.
     * </p>
     */
    @Override
    public MongoClientMetrics getMetrics() {
        return myProxiedConnectionFactory.getMetrics();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the delegates strategy but replace his connection
     * factory with our own.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        final ReconnectStrategy delegates = myProxiedConnectionFactory
                .getReconnectStrategy();

        delegates.setConnectionFactory(this);

        return delegates;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets the metrics agent on the proxied connection factory.
     * </p>
     */
    @Override
    public void setMetrics(final MongoClientMetrics metrics) {
        myProxiedConnectionFactory.setMetrics(metrics);
    }
}
