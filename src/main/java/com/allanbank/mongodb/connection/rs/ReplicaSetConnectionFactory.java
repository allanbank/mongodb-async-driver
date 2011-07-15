/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * Provides the ability to create connections to a replica-set environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactory implements ConnectionFactory {

	/** The state of the cluster. */
	private final ClusterState myClusterState;

	/** The factory to create proxied connections. */
	protected final ProxiedConnectionFactory myConnectionFactory;

	/** The MongoDB client configuration. */
	private final MongoDbConfiguration myConfig;

	/**
	 * Creates a new {@link ReplicaSetConnectionFactory}.
	 * 
	 * @param factory
	 *            The factory to create proxied connections.
	 * @param config
	 *            The MongoDB client configuration.
	 */
	public ReplicaSetConnectionFactory(ProxiedConnectionFactory factory,
			final MongoDbConfiguration config) {
		myConnectionFactory = factory;
		myConfig = config;
		myClusterState = new ClusterState();
		for (final InetSocketAddress address : config.getServers()) {
			final ServerState state = myClusterState.add(address.toString());

			// In a replica-set environment we assume that all of the
			// servers are non-writable.
			myClusterState.markNotWritable(state);
		}

		// TODO - Bootstrap the state off of one of the servers.
	}

	/**
	 * Creates a new connection to the replica set.
	 * 
	 * @see ConnectionFactory#connect()
	 */
	@Override
	public Connection connect() throws IOException {
		return new ReplicaSetConnection(myConnectionFactory, myClusterState,
				myConfig);
	}

}
