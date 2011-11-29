/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * Provides a {@link Connection} implementation for connecting to a replica-set
 * environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnection extends AbstractProxyConnection {

    /** A connection to a Secondary Replica. */
    private final Connection mySecondaryConnection;

    /**
     * Creates a new {@link ReplicaSetConnection}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param clusterState
     *            The state of the cluster.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnection(final ProxiedConnectionFactory factory,
            final ClusterState clusterState, final MongoDbConfiguration config) {
        super(factory, clusterState, config);
        mySecondaryConnection = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Issues a { ismaster : 1 } command on the 'admin' database and updates the
     * cluster state with the results.
     * </p>
     * 
     * @return True if the connection is to the primary replica and false for a
     *         secondary replica.
     */
    @Override
    protected boolean verifyConnection(final Connection connection) {
        // TODO Auto-generated method stub
        return false;
    }

}
