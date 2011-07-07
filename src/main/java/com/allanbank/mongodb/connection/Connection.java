/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.io.Closeable;
import java.io.Flushable;
import java.util.List;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;

/**
 * Provides the lowest level interface for interacting with a MongoDB server.
 * The method provided here are a straight forward mapping from the <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol">MongoDB Wire
 * Protocol</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Connection extends Closeable, Flushable {
	/** The collection to use when issuing commands to the database. */
	public static final String COMMAND_COLLECTION = "$cmd";

	/**
	 * Sends a <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPDELETE"
	 * >delete</a> command to the connected MongoDB server.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param query
	 *            The query document for selecting documents to delete.
	 * @param multiDelete
	 *            If true multiple documents matching the query will be deleted.
	 *            Otherwise only the first document is deleted.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public void delete(String dbName, String collectionName, Document query,
			boolean multiDelete) throws MongoDbException;

	/**
	 * Sends a <a href=
	 * "http://www.mongodb.org/display/DOCS/getLastError+Command"
	 * >getlasterror</a> command to the connected MongoDB server.
	 * <p>
	 * This is a helper method for retrieving the results of {@link #delete},
	 * {@link #insert}, and {@link #update} commands. Get last update is not a
	 * part of the standard wire protocol but is provided here due to the
	 * frequency of usage.
	 * </p>
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param fsync
	 *            If true the command waits for an fsync of the data to have
	 *            completed.
	 * @param waitForJournal
	 *            If true the command waits for the preceding command to have
	 *            been written to the journal.
	 * @param w
	 *            The replication factor to wait for.
	 * @param wtimeout
	 *            The amount of time (in milliseconds) to wait for the write to
	 *            finish.
	 * @return The request id assigned to this query. Can be used to correlate
	 *         to a read response.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public int getLastError(final String dbName, final boolean fsync,
			final boolean waitForJournal, final int w, final int wtimeout)
			throws MongoDbException;

	/**
	 * Sends a <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPGETMORE"
	 * >getmore</a> command to the connected MongoDB server.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param cursorId
	 *            The id of the cursor to get more data from.
	 * @param numberToReturn
	 *            The number of documents to return in this batch.
	 * @return The request id assigned to this query. Can be used to correlate
	 *         to a read response.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public int getMore(String dbName, String collectionName, long cursorId,
			int numberToReturn) throws MongoDbException;

	/**
	 * Sends an <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPINSERT"
	 * >insert</a> command to the connected MongoDB server.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param documents
	 *            The documents to be inserted.
	 * @param keepGoing
	 *            If true then a failure on a single document will not stop all
	 *            of the other documents from being inserted.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public void insert(String dbName, String collectionName,
			List<Document> documents, boolean keepGoing)
			throws MongoDbException;

	/**
	 * Sends a <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPKILLCURSORS"
	 * >killcursor</a> command to the connected MongoDB server.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param cursorId
	 *            The id of the cursor to kill/delete.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public void killCursor(String dbName, String collectionName, long cursorId)
			throws MongoDbException;

	/**
	 * Sends a <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPQUERY"
	 * >query</a> command to the connected MongoDB server. Most
	 * <em>commands</em> to the MongoDB server are actually queries to the
	 * <tt>$cmd</tt> Collection.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param query
	 *            The query to use in selecting documents to return.
	 * @param returnFields
	 *            The return fields document, may be null.
	 * @param numberToReturn
	 *            The number of documents to be returned.
	 * @param numberToSkip
	 *            The number of documents to skip before starting to return
	 *            documents.
	 * @param tailable
	 *            If true then a 'tailable' cursor should be created.
	 * @param slaveOk
	 *            If true then executing the query on a slave allowed.
	 * @param noCursorTimeout
	 *            If true then the cursor created will not timeout. Use with
	 *            caution.
	 * @param awaitData
	 *            If using a tailable cursor then the connection will block
	 *            waiting for more data.
	 * @param exhaust
	 *            All results should be returned in multiple results.
	 * @param partial
	 *            Return the results found and suppress shard down errors.
	 * @return The request id assigned to this query. Can be used to correlate
	 *         to a read response.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public int query(String dbName, String collectionName, Document query,
			Document returnFields, int numberToReturn, int numberToSkip,
			boolean tailable, boolean slaveOk, boolean noCursorTimeout,
			boolean awaitData, boolean exhaust, boolean partial)
			throws MongoDbException;

	/**
	 * Reads a <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPREPLY"
	 * >reply</a> message from the connected MongoDB server. This method may
	 * block until a reply is received or an error occurs.
	 * 
	 * @return The received reply or null if there is no reply.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public Reply read() throws MongoDbException;

	/**
	 * Sends an <a href=
	 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPUPDATE"
	 * >update</a> command to the connected MongoDB server.
	 * 
	 * @param dbName
	 *            The name of the database.
	 * @param collectionName
	 *            The name of the collection.
	 * @param query
	 *            The query to select document to update.
	 * @param update
	 *            The update to be applied to the documents.
	 * @param upsert
	 *            If there are no matches to the document then create one.
	 * @param multiUpdate
	 *            If true multiple documents may be updated. Otherwise only the
	 *            first document is updated.
	 * @throws MongoDbException
	 *             On an error communicating with the MongoDB server.
	 */
	public void update(String dbName, String collectionName, Document query,
			Document update, boolean upsert, boolean multiUpdate)
			throws MongoDbException;

}
