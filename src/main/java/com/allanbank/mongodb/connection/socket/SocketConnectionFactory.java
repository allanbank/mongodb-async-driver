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
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;

/**
 * {@link ConnectionFactory} to create direct socket connections to the servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactory implements ConnectionFactory {

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
		MongoDbException last = null;
		Collections.shuffle(servers);
		for (final InetSocketAddress address : servers) {
			try {
				return new SocketConnection(address, myConfig);
			} catch (final MongoDbException error) {
				last = error;
			}
		}

		if (last != null) {
			throw last;
		}
		throw new MongoDbException("Could not connect to any server: "
				+ servers);
	}

}
