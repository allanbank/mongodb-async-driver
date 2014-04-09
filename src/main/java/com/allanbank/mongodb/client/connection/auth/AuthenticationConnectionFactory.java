/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.auth;

import java.io.IOException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.ClusterStats;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
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
public class AuthenticationConnectionFactory implements
        ProxiedConnectionFactory {

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
}
