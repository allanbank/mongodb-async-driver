/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.bootstrap;

import java.io.IOException;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;

/**
 * Provides the ability to bootstrap into the appropriate
 * {@link ConnectionFactory} based on the configuration of the server(s)
 * connected to.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactory implements ConnectionFactory {

	/** The delegate connection factory post */
	private final ConnectionFactory myDelegate = null;

	/**
	 * Creates a {@link BootstrapConnectionFactory}
	 * 
	 * @param config
	 *            The configuration to use in discovering the server
	 *            configuration.
	 * @throws IOException
	 *             If the bootstrap fails.
	 */
	public BootstrapConnectionFactory(final MongoDbConfiguration config)
			throws IOException {
		bootstrap(config);
	}

	/**
	 * Re-bootstraps the environment. Normally this method is only called once
	 * during the constructor of the factory to initialize the delegate but
	 * users can reset the delegate by manually invoking this method.
	 * 
	 * @param config
	 *            The configuration to use in discovering the server
	 *            configuration.
	 * 
	 * @throws IOException
	 *             If the bootstrap fails.
	 */
	public void bootstrap(final MongoDbConfiguration config) throws IOException {
		// TODO - IMPLEMENT BOOTSTRAP.
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Delegates the connection to the setup delegate.
	 * </p>
	 */
	@Override
	public Connection connect() throws IOException {
		return myDelegate.connect();
	}
}
