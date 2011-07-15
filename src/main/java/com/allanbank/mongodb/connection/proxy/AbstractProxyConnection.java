/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Reply;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * A helper class for constructing connections that are really just proxies on
 * top of other connections.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyConnection implements Connection {
	/** The factory to create proxied connections. */
	protected final ProxiedConnectionFactory myConnectionFactory;

	/** The state of the cluster. */
	protected final ClusterState myClusterState;

	/** The MongoDB client configuration. */
	protected final MongoDbConfiguration myConfig;

	/** The proxied connection. */
	private Connection myProxiedConnection;

	/**
	 * Creates a AbstractProxyConnection.
	 * 
	 * @param factory
	 *            The factory to create proxied connections.
	 * @param clusterState
	 *            The state of the cluster.
	 * @param config
	 *            The MongoDB client configuration.
	 */
	public AbstractProxyConnection(ProxiedConnectionFactory factory,
			final ClusterState clusterState, final MongoDbConfiguration config) {
		myConnectionFactory = factory;
		myClusterState = clusterState;
		myConfig = config;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#delete(String, String, Document, boolean)
	 */
	@Override
	public void delete(final String dbName, final String collectionName,
			final Document query, final boolean multiDelete)
			throws MongoDbException {
		try {
			ensureConnected()
					.delete(dbName, collectionName, query, multiDelete);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see java.io.Flushable#flush()
	 */
	@Override
	public void flush() throws IOException {
		try {
			ensureConnected().flush();
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#getLastError(String , boolean, boolean, int, int)
	 */
	@Override
	public int getLastError(final String dbName, final boolean fsync,
			final boolean waitForJournal, final int w, final int wtimeout)
			throws MongoDbException {
		try {
			return ensureConnected().getLastError(dbName, fsync,
					waitForJournal, w, wtimeout);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#getMore(String, String, long, int)
	 */
	@Override
	public int getMore(final String dbName, final String collectionName,
			final long cursorId, final int numberToReturn)
			throws MongoDbException {
		try {
			return ensureConnected().getMore(dbName, collectionName, cursorId,
					numberToReturn);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#insert(String, String, List, boolean)
	 */
	@Override
	public void insert(final String dbName, final String collectionName,
			final List<Document> documents, final boolean keepGoing)
			throws MongoDbException {
		try {
			ensureConnected().insert(dbName, collectionName, documents,
					keepGoing);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#killCursor(String, String, long)
	 */
	@Override
	public void killCursor(final String dbName, final String collectionName,
			final long cursorId) throws MongoDbException {
		try {
			ensureConnected().killCursor(dbName, collectionName, cursorId);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#query(String, String, Document, Document, int, int,
	 *      boolean, boolean, boolean, boolean, boolean, boolean)
	 */
	@Override
	public int query(final String dbName, final String collectionName,
			final Document query, final Document returnFields,
			final int numberToReturn, final int numberToSkip,
			final boolean tailable, final boolean slaveOk,
			final boolean noCursorTimeout, final boolean awaitData,
			final boolean exhaust, final boolean partial)
			throws MongoDbException {
		try {
			return ensureConnected().query(dbName, collectionName, query,
					returnFields, numberToReturn, numberToSkip, tailable,
					slaveOk, noCursorTimeout, awaitData, exhaust, partial);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#read()
	 */
	@Override
	public Reply read() throws MongoDbException {
		try {
			return ensureConnected().read();
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Forwards the call to the {@link Connection} returned from
	 * {@link #ensureConnected()}.
	 * </p>
	 * 
	 * @see Connection#update(String, String, Document, Document, boolean,
	 *      boolean)
	 */
	@Override
	public void update(final String dbName, final String collectionName,
			final Document query, final Document update, final boolean upsert,
			final boolean multiUpdate) throws MongoDbException {
		try {
			ensureConnected().update(dbName, collectionName, query, update,
					upsert, multiUpdate);
		} catch (MongoDbException error) {
			onExceptin(error);
			throw error;
		}
	}

	/**
	 * Closes the underlying connection.
	 * 
	 * @see Connection#close()
	 */
	@Override
	public void close() throws IOException {
		if (myProxiedConnection != null) {
			myProxiedConnection.close();
			myProxiedConnection = null;
		}
	}

	/**
	 * Ensure that the proxied connection is cconnected and return the
	 * Connection to proxy the call to.
	 * <p>
	 * Checks if already connected. If not creates a new connection to a server.
	 * </p>
	 * 
	 * @return The {@link Connection} to forward a call to.
	 * @throws MongoDbException
	 *             On a failure establishing a connection.
	 */
	protected Connection ensureConnected() throws MongoDbException {
		if (myProxiedConnection == null) {
			connect();
		}
		return myProxiedConnection;
	}

	/**
	 * Provides the ability for derived classes to intercept any exceptions from
	 * the underlying proxied connection.
	 * <p>
	 * Closes the underlying connection.
	 * </p>
	 * 
	 * @param exception
	 *            The thrown exception.
	 */
	protected void onExceptin(MongoDbException exception) {
		try {
			close();
		} catch (IOException e) {
			myProxiedConnection = null;
		}
	}

	/**
	 * Connects to the cluster.
	 */
	protected void connect() {
		final List<ServerState> servers = myClusterState.getWritableServers();

		// Shuffle the servers and try to connect to each until one works.
		Exception last = null;
		Collections.shuffle(servers);
		for (final ServerState server : servers) {
			Connection connection = null;
			try {
				connection = myConnectionFactory.connect(server.getServer(),
						myConfig);

				if (verifyConnection(connection)) {

					myProxiedConnection = connection;
					connection = null;

					return;
				}
			} catch (MongoDbException error) {
				last = error;
			} catch (IOException error) {
				last = error;
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (IOException ignored) {
						// Nothing.
					}
				}
			}
		}

		if (last != null) {
			throw new MongoDbException("Could not connect to any server: "
					+ servers, last);
		}
		throw new MongoDbException("Could not connect to any server: "
				+ servers);
	}

	/**
	 * Verifies that the connection is working. The connection passed is newly
	 * created and can safely be assumed to not have been used for any purposes
	 * or have any message sent of received.
	 * 
	 * @param connection
	 *            The connection to verify.
	 * @return True if the connection has been verified.
	 * @throws MongoDbException
	 *             On a failure verifying the connection.
	 */
	protected abstract boolean verifyConnection(Connection connection)
			throws MongoDbException;
}
