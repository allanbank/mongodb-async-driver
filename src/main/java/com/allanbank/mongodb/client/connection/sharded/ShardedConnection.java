/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.sharded;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyConnection;

/**
 * Provides a {@link Connection} implementation for connecting to a sharded
 * environment via mongos servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnection extends AbstractProxyConnection {
    /**
     * Creates a new {@link ShardedConnection}.
     * 
     * @param proxiedConnection
     *            The connection being proxied.
     * @param config
     *            The MongoDB client configuration.
     */
    public ShardedConnection(final Connection proxiedConnection,
            final MongoClientConfiguration config) {
        super(proxiedConnection);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "Sharded(" + getProxiedConnection() + ")";
    }
}
