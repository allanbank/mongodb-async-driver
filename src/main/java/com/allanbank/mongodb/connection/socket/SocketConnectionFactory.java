/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * {@link ConnectionFactory} to create direct socket connections to the servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactory implements ConnectionFactory,
		ProxiedConnectionFactory {

	/** The MongoDB client configuration. */
	private final MongoDbConfiguration myConfig;

	/**
	 * Creates a new {@link SocketConnectionFactory}.
	 * 
	 * @param config
	 *            The MongoDB client configuration.
	 */
	public SocketConnectionFactory(final MongoDbConfiguration config) {
		super();
		myConfig = config;
	}

	/**
	 * Creates a connection to the address provided.
	 * 
	 * @param address
	 *            The address of the MongoDB server to connect to.
	 * @param config
	 *            The configuration for the Connection to the MongoDB server.
	 * @return The Connection to MongoDB.
	 * @throws IOException
	 *             On a failure connecting to the server.
	 */
	@Override
	public Connection connect(InetSocketAddress address,
			MongoDbConfiguration config) throws IOException {
		return new SocketConnection(address, myConfig);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Returns a new {@link SocketConnection}.
	 * </p>
	 * 
	 * @see ConnectionFactory#connect()
	 */
	@Override
	public Connection connect() throws IOException {
		final List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>(
				myConfig.getServers());

		// Shuffle the servers and try to connect to each until one works.
		IOException last = null;
		Collections.shuffle(servers);
		for (final InetSocketAddress address : servers) {
			try {
				return connect(address, myConfig);
			} catch (final IOException error) {
				last = error;
			}
		}

		if (last != null) {
			throw last;
		}
		throw new IOException("Could not connect to any server: " + servers);
	}

}
