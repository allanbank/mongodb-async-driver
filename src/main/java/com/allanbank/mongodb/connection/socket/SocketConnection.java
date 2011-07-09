/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.BsonReader;
import com.allanbank.mongodb.bson.io.BsonWriter;
import com.allanbank.mongodb.bson.io.RandomAccessOutputStream;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.Reply;

/**
 * Provides a simple blocking Socket based connection to a MongoDB server.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnection implements Connection {

	/** The length of the message header in bytes. */
	public static final int HEADER_LENGTH = 16;

	/** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
	private final BsonWriter myDocumentWriter;

	/** The buffered input stream. */
	private final BufferedInputStream myInput;

	/** The next message id. */
	private int myNextId;

	/** The buffer for constructing the messages. */
	private final RandomAccessOutputStream myOutBuffer;

	/** The buffered output stream. */
	private final BufferedOutputStream myOutput;

	/** The writer for BSON documents. Shares this objects {@link #myOutBuffer}. */
	private final BsonReader myReader;

	/** The open socket. */
	private final Socket mySocket;

	/**
	 * Creates a new SocketConnection to a MongoDB server.
	 * 
	 * @param address
	 *            The address of the MongoDB server to connect to.
	 * @param config
	 *            The configuration for the Connection to the MongoDB server.
	 * @throws SocketException
	 *             On a failure connecting to the MongoDB server.
	 * @throws IOException
	 *             On a failure to read or write data to the MongoDB server.
	 */
	public SocketConnection(final InetSocketAddress address,
			final MongoDbConfiguration config) throws SocketException,
			IOException {

		myOutBuffer = new RandomAccessOutputStream();
		mySocket = new Socket();

		mySocket.setKeepAlive(config.isUsingSoKeepalive());
		mySocket.setSoTimeout(config.getReadTimeout());
		mySocket.connect(address, config.getConnectTimeout());

		myInput = new BufferedInputStream(mySocket.getInputStream());
		myReader = new BsonReader(myInput);

		myOutput = new BufferedOutputStream(mySocket.getOutputStream());
		myDocumentWriter = new BsonWriter(myOutBuffer);

		myNextId = 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		myOutput.close();
		myInput.close();
		mySocket.close();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct {
	 *     MsgHeader header;             // standard message header
	 *     int32     ZERO;               // 0 - reserved for future use
	 *     cstring   fullCollectionName; // "dbname.collectionname"
	 *     int32     flags;              // bit vector - see below for details.
	 *     document  selector;           // query object.  See below for details.
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public void delete(final String dbName, final String collectionName,
			final Document query, final boolean multiDelete)
			throws MongoDbException {

		int flags = 0;
		if (!multiDelete) {
			flags += 1;
		}

		final long start = writeHeader(myNextId++, Operation.DELETE);
		myOutBuffer.writeInt(0);
		myOutBuffer.writeCString(dbName, ".", collectionName);
		myOutBuffer.writeInt(flags);

		try {
			myDocumentWriter.write(query);

			// re-write the document size.
			final int size = (int) (myOutBuffer.getPosition() - start);
			myOutBuffer.writeIntAt(start, size);

			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException {
		myOutput.flush();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getLastError(final String dbName, final boolean fsync,
			final boolean waitForJournal, final int w, final int wtimeout)
			throws MongoDbException {
		final DocumentBuilder builder = BuilderFactory.start();
		builder.addInteger("getlasterror", 1);
		if (waitForJournal) {
			builder.addBoolean("j", waitForJournal);
		}
		if (fsync) {
			builder.addBoolean("fsync", fsync);
		}
		if (w > 1) {
			builder.addInteger("w", w);
		}
		if (wtimeout > 0) {
			builder.addInteger("wtimeout", wtimeout);
		}

		return query(dbName, COMMAND_COLLECTION, builder.get(), null, 1, 0,
				false, false, false, false, false, false);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct {
	 *     MsgHeader header;             // standard message header
	 *     int32     ZERO;               // 0 - reserved for future use
	 *     cstring   fullCollectionName; // "dbname.collectionname"
	 *     int32     numberToReturn;     // number of documents to return
	 *     int64     cursorID;           // cursorID from the OP_REPLY
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public int getMore(final String dbName, final String collectionName,
			final long cursorId, final int numberToReturn)
			throws MongoDbException {
		final int requestId = myNextId++;

		final long start = writeHeader(requestId, Operation.GET_MORE);
		myOutBuffer.writeInt(0);
		myOutBuffer.writeCString(dbName, ".", collectionName);
		myOutBuffer.writeInt(numberToReturn);
		myOutBuffer.writeLong(cursorId);

		try {
			// re-write the document size.
			final int size = (int) (myOutBuffer.getPosition() - start);
			myOutBuffer.writeIntAt(start, size);

			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
		return requestId;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct {
	 *     MsgHeader header;             // standard message header
	 *     int32     flags;              // bit vector - see below
	 *     cstring   fullCollectionName; // "dbname.collectionname"
	 *     document* documents;          // one or more documents to insert into the collection
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public void insert(final String dbName, final String collectionName,
			final List<Document> documents, final boolean keepGoing)
			throws MongoDbException {
		int flags = 0;
		if (!keepGoing) {
			flags += 1;
		}

		final long start = writeHeader(myNextId++, Operation.INSERT);
		myOutBuffer.writeInt(flags);
		myOutBuffer.writeCString(dbName, ".", collectionName);

		try {
			for (final Document doc : documents) {
				myDocumentWriter.write(doc);
			}

			// re-write the document size.
			final int size = (int) (myOutBuffer.getPosition() - start);
			myOutBuffer.writeIntAt(start, size);

			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct {
	 *     MsgHeader header;            // standard message header
	 *     int32     ZERO;              // 0 - reserved for future use
	 *     int32     numberOfCursorIDs; // number of cursorIDs in message
	 *     int64*    cursorIDs;         // sequence of cursorIDs to close
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public void killCursor(final String dbName, final String collectionName,
			final long cursorId) throws MongoDbException {
		writeHeader(myNextId++, Operation.KILL_CURSORS, HEADER_LENGTH + 16);
		myOutBuffer.writeInt(0);
		myOutBuffer.writeInt(1);
		myOutBuffer.writeLong(cursorId);

		try {
			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct OP_QUERY {
	 *     MsgHeader header;                // standard message header
	 *     int32     flags;                  // bit vector of query options.  See below for details.
	 *     cstring   fullCollectionName;    // "dbname.collectionname"
	 *     int32     numberToSkip;          // number of documents to skip
	 *     int32     numberToReturn;        // number of documents to return
	 *                                      //  in the first OP_REPLY batch
	 *     document  query;                 // query object. 
	 *   [ document  returnFieldSelector; ] // Optional. Selector indicating the fields
	 *                                      //  to return.  
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public int query(final String dbName, final String collectionName,
			final Document query, final Document returnFields,
			final int numberToReturn, final int numberToSkip,
			final boolean tailable, final boolean slaveOk,
			final boolean noCursorTimeout, final boolean awaitData,
			final boolean exhaust, final boolean partial)
			throws MongoDbException {

		// Flag bits.
		// 1 TailableCursor
		// 2 SlaveOk
		// 3 OplogReplay
		// 4 NoCursorTimeout
		// 5 AwaitData
		// 6 Exhaust
		// 7 Partial
		int flags = 0;
		if (tailable) {
			flags += 0x02;
		}
		if (slaveOk) {
			flags += 0x04;
		}
		// OplodReply - slave only.
		if (noCursorTimeout) {
			flags += 0x10;
		}
		if (awaitData) {
			flags += 0x20;
		}
		if (exhaust) {
			flags += 0x40;
		}
		if (partial) {
			flags += 0x80;
		}

		final int requestId = myNextId++;

		final long start = writeHeader(requestId, Operation.QUERY);
		myOutBuffer.writeInt(flags);
		myOutBuffer.writeCString(dbName, ".", collectionName);
		myOutBuffer.writeInt(numberToSkip);
		myOutBuffer.writeInt(numberToReturn);

		try {
			myDocumentWriter.write(query);
			if (returnFields != null) {
				myDocumentWriter.write(returnFields);
			}
			// re-write the document size.
			final int size = (int) (myOutBuffer.getPosition() - start);
			myOutBuffer.writeIntAt(start, size);

			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
		return requestId;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct MsgHeader {
	 *     int32   messageLength; // total message size, including this
	 *     int32   requestID;     // identifier for this message
	 *     int32   responseTo;    // requestID from the original request
	 *                            //   (used in reponses from db)
	 *     int32   opCode;        // request type - see table below
	 * }
	 * struct {
	 *     MsgHeader header;         // standard message header
	 *     int32     responseFlags;  // bit vector - see details below
	 *     int64     cursorID;       // cursor id if client needs to do get more's
	 *     int32     startingFrom;   // where in the cursor this reply is starting
	 *     int32     numberReturned; // number of documents in the reply
	 *     document* documents;      // documents
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public Reply read() throws MongoDbException {
		try {
			myReader.readInt(); // length not needed for a blocking impl.
			myReader.readInt(); // requestId - not needed.
			final int responseTo = myReader.readInt();
			final int opCode = myReader.readInt();

			if (opCode != Operation.REPLY.getCode()) {
				// Huh? Dazed and confused
				throw new MongoDbException("Unexpected operation read '"
						+ opCode + "', expected '" + Operation.REPLY.getCode()
						+ "'.");
			}

			final int flags = myReader.readInt();
			final long cursorId = myReader.readLong();
			final int startingIndex = myReader.readInt();
			final int numberReturned = myReader.readInt();

			final List<Document> results = new ArrayList<Document>(
					numberReturned);
			for (int i = 0; i < numberReturned; ++i) {
				results.add(myReader.readDocument());
			}

			return new Reply(responseTo, flags, cursorId, startingIndex,
					results);
		} catch (final IOException ioe) {
			throw new MongoDbException(ioe);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <pre>
	 * <code>
	 * struct OP_UPDATE {
	 *     MsgHeader header;             // standard message header
	 *     int32     ZERO;               // 0 - reserved for future use
	 *     cstring   fullCollectionName; // "dbname.collectionname"
	 *     int32     flags;              // bit vector. see below
	 *     document  selector;           // the query to select the document
	 *     document  update;             // specification of the update to perform
	 * }
	 * </code>
	 * </pre>
	 */
	@Override
	public void update(final String dbName, final String collectionName,
			final Document query, final Document update, final boolean upsert,
			final boolean multiUpdate) throws MongoDbException {
		// Flag bits
		// 0 Upsert
		// 1 MultiUpdate
		int flags = 0;
		if (upsert) {
			flags += 0x01;
		}
		if (multiUpdate) {
			flags += 0x02;
		}

		final long start = writeHeader(myNextId++, Operation.UPDATE);
		myOutBuffer.writeInt(0);
		myOutBuffer.writeCString(dbName, ".", collectionName);
		myOutBuffer.writeInt(flags);

		try {
			myDocumentWriter.write(query);
			myDocumentWriter.write(update);

			// re-write the document size.
			final int size = (int) (myOutBuffer.getPosition() - start);
			myOutBuffer.writeIntAt(start, size);

			// Now stream out.
			myOutBuffer.writeTo(myOutput);

			// Reset so we can reuse the allocated buffers.
			myOutBuffer.reset();
		} catch (final IOException error) {
			throw new MongoDbException(error);
		}
	}

	/**
	 * Writes the MsgHeader to the {@link #myOutBuffer}.
	 * 
	 * <pre>
	 * <code>
	 * struct MsgHeader {
	 *     int32   messageLength; // total message size, including this
	 *     int32   requestID;     // identifier for this message
	 *     int32   responseTo;    // requestID from the original request
	 *                            //   (used in reponses from db)
	 *     int32   opCode;        // request type - see table below
	 * }
	 * </code>
	 * </pre>
	 * 
	 * @param id
	 *            THe requestID from above.
	 * @param op
	 *            The operation for the opCode field.
	 * @return The start position of the buffer for re-writing the
	 *         messageLength.
	 */
	protected long writeHeader(final int id, final Operation op) {
		return writeHeader(id, op, 0);
	}

	/**
	 * Writes the MsgHeader to the {@link #myOutBuffer}.
	 * 
	 * <pre>
	 * <code>
	 * struct MsgHeader {
	 *     int32   messageLength; // total message size, including this
	 *     int32   requestID;     // identifier for this message
	 *     int32   responseTo;    // requestID from the original request
	 *                            //   (used in reponses from db)
	 *     int32   opCode;        // request type - see table below
	 * }
	 * </code>
	 * </pre>
	 * 
	 * @param id
	 *            THe requestID from above.
	 * @param op
	 *            The operation for the opCode field.
	 * @param length
	 *            The length of the message including the header.
	 * @return The start position of the buffer for re-writing the
	 *         messageLength.
	 */
	protected long writeHeader(final int id, final Operation op,
			final int length) {
		final long start = myOutBuffer.getPosition();

		myOutBuffer.writeInt(length);
		myOutBuffer.writeInt(id);
		myOutBuffer.writeInt(0);
		myOutBuffer.writeInt(op.getCode());

		return start;
	}

}
