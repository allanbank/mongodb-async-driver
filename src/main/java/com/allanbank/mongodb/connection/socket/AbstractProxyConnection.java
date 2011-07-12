/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import java.io.IOException;
import java.util.List;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Reply;

/**
 * A helper class for constructing connections that are really just proxies on
 * top of other connections.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractProxyConnection implements Connection {

	/**
	 * Creates a AbstractProxyConnection.
	 */
	public AbstractProxyConnection() {
		super();
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
		ensureConnected().delete(dbName, collectionName, query, multiDelete);
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
		ensureConnected().flush();
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
		return ensureConnected().getLastError(dbName, fsync, waitForJournal, w,
				wtimeout);
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
		return ensureConnected().getMore(dbName, collectionName, cursorId,
				numberToReturn);
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
		ensureConnected().insert(dbName, collectionName, documents, keepGoing);
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
		ensureConnected().killCursor(dbName, collectionName, cursorId);
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
		return ensureConnected().query(dbName, collectionName, query,
				returnFields, numberToReturn, numberToSkip, tailable, slaveOk,
				noCursorTimeout, awaitData, exhaust, partial);
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
		return ensureConnected().read();
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
		ensureConnected().update(dbName, collectionName, query, update, upsert,
				multiUpdate);
	}

	/**
	 * Ensure that the proxied connection is cconnected and return the
	 * Connection to proxy the call to.
	 * 
	 * @return The {@link Connection} to forward a call to.
	 * @throws MongoDbException
	 *             On a failure establishing a connection.
	 */
	protected abstract Connection ensureConnected() throws MongoDbException;
}
