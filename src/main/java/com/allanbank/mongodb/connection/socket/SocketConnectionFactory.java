/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;

/**
 * {@link ConnectionFactory} to create direct socket connections to the servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactory implements ConnectionFactory {

	/**
	 * Creates a new {@link SocketConnectionFactory}.
	 */
	public SocketConnectionFactory() {
		super();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Returns a new {@link SocketConnection}.
	 * </p>
	 * 
	 * @see ConnectionFactory#connect(InetSocketAddress,MongoDbConfiguration)
	 */
	@Override
	public Connection connect(final InetSocketAddress address,
			final MongoDbConfiguration config) throws IOException {
		return new SocketConnection(address, config);
	}

}
