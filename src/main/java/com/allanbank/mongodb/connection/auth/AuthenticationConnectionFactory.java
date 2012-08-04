/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * AuthenticationConnectionFactory wraps all of the connections with
 * {@link AuthenticatingConnection}s.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticationConnectionFactory implements
        ProxiedConnectionFactory {

    /** The default config. */
    private final MongoDbConfiguration myConfig;

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
            final MongoDbConfiguration config) {
        myProxiedConnectionFactory = factory;
        myConfig = config;
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
    public AuthenticatingConnection connect(final InetSocketAddress address,
            final MongoDbConfiguration config) throws IOException {
        return new AuthenticatingConnection(myProxiedConnectionFactory.connect(
                address, config), config);
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
