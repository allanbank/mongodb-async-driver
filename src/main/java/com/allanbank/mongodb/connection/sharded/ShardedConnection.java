/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * Provides a {@link Connection} implementation for connecting to a sharded
 * environment via mongos servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnection extends AbstractProxyConnection {

	/**
	 * Creates a new {@link ShardedConnection}.
	 * 
	 * @param factory
	 *            The factory to create proxied connections.
	 * @param clusterState
	 *            The state of the cluster.
	 * @param config
	 *            The MongoDB client configuration.
	 */
	public ShardedConnection(ProxiedConnectionFactory factory,
			final ClusterState clusterState, final MongoDbConfiguration config) {
		super(factory, clusterState, config);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Issues a { ismaster : 1 } command on the 'admin' database and verifies
	 * that the response is from a mongos.
	 * </p>
	 */
	@Override
	protected boolean verifyConnection(Connection connection)
			throws MongoDbException {
		// TODO Auto-generated method stub
		return true;
	}
}
