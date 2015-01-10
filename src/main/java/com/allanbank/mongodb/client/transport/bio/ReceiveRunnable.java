/*
 * #%L
 * ReceiveRunnable.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.client.transport.bio;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StreamCorruptedException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.logging.Level;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Header;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Runnable to receive messages from an {@link AbstractSocketTransport}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReceiveRunnable implements Runnable, Receiver {
	/** The logger for the receive thread. */
	private static final Log LOG = LogFactory.getLog(ReceiveRunnable.class);

	/** The configuration for the client. */
	private final MongoClientConfiguration myConfig;

	/** The transport. */
	private final AbstractSocketTransport<?> myTransport;

	/** The input to read from. */
	private final BsonInputStream myBsonIn;

	/** The listener for responses from the server. */
	private final TransportResponseListener myResponseListener;

	/** Tracks the number of sequential read timeouts. */
	private int myIdleTicks = 0;

	/** Tracks the address for the remote/far/other end of the socket. */
	private final SocketAddress myRemoteAddress;

	/**
	 * Creates a new ReceiveRunnable.
	 * 
	 * @param config
	 *            The configuration for the client.
	 * @param transport
	 *            The socket we are reading from.
	 */
	public ReceiveRunnable(MongoClientConfiguration config,
			final AbstractSocketTransport<?> transport) {
		myConfig = config;
		myTransport = transport;
		myBsonIn = transport.getBsonIn();
		myResponseListener = transport.getResponseListener();
		myRemoteAddress = transport.getRemoteAddress();
	}

	/**
	 * Processing thread for receiving responses from the server.
	 */
	@Override
	public void run() {
		try {
			while (myTransport.isOpen()) {
				try {
					doReceiveOne();

					// Check if we are shutdown. Note the shutdown() method
					// makes sure the last message gets a reply.
					if (myTransport.isShuttingDown() && myTransport.isIdle()) {
						// All done.
						return;
					}
				} catch (final MongoDbException error) {
					if (myTransport.isOpen()) {
						LOG.log(Level.WARNING, "Error reading a message: "
								+ error.getMessage(), error);

						myTransport.shutdown(
								new ConnectionLostException(error), false);
					}
					// All done.
					return;
				}
			}
		} finally {
			// Make sure the connection is closed completely.
			IOUtils.close(myTransport);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * If there is a pending flush then flushes.
	 * </p>
	 * <p>
	 * If there is any available data then does a single receive.
	 * </p>
	 */
	@Override
	public void tryReceive() {
		try {
			if (myBsonIn.available() > 0) {
				doReceiveOne();
			}
		} catch (final IOException error) {
			LOG.info(
					"Received an error when checking for pending messages: {}.",
					error.getMessage());
		}
	}

	/**
	 * Receives and process a single message.
	 */
	protected void doReceiveOne() {

		final Message received = doReceive();
		if (received != null) {
			myIdleTicks = 0;
			handle(received);
		} else {
			myIdleTicks += 1;

			if (myConfig.getMaxIdleTickCount() <= myIdleTicks) {
				// Shutdown the connection., nicely.
				myTransport.shutdown(new ConnectionLostException(
						"Connection closed due to idle."), false);
			}
		}
	}

	/**
	 * Process a single reply.
	 * 
	 * @param reply
	 *            The received reply.
	 */
	protected void handle(final Message reply) {
		myResponseListener.response(new MessageInputBuffer(reply));
	}

	/**
	 * Reads a little-endian 4 byte signed integer from the stream.
	 * 
	 * @return The integer value.
	 * @throws EOFException
	 *             On insufficient data for the integer.
	 * @throws IOException
	 *             On a failure reading the integer.
	 */
	protected int readIntSuppressTimeoutOnNonFirstByte() throws EOFException,
			IOException {
		int read = 0;
		int eofCheck = 0;
		int result = 0;

		read = myBsonIn.read();
		eofCheck |= read;
		result += (read << 0);

		for (int i = Byte.SIZE; i < Integer.SIZE; i += Byte.SIZE) {
			try {
				read = myBsonIn.read();
			} catch (final SocketTimeoutException ste) {
				// Bad - Only the first byte should timeout.
				throw new IOException(ste);
			}
			eofCheck |= read;
			result += (read << i);
		}

		if (eofCheck < 0) {
			throw new EOFException("Remote connection closed: "
					+ myRemoteAddress);
		}
		return result;
	}

	/**
	 * Receives a single message from the connection.
	 * 
	 * @return The {@link Message} received.
	 * @throws MongoDbException
	 *             On an error receiving the message.
	 */
	protected Message doReceive() throws MongoDbException {
		try {
			int length;
			try {
				length = readIntSuppressTimeoutOnNonFirstByte();
			} catch (final SocketTimeoutException ok) {
				// This is OK. We check if we are still running and come right
				// back.
				return null;
			}

			myBsonIn.prefetch(length - 4);

			final int requestId = myBsonIn.readInt();
			final int responseId = myBsonIn.readInt();
			final int opCode = myBsonIn.readInt();

			final Operation op = Operation.fromCode(opCode);
			if (op == null) {
				// Huh? Dazed and confused
				throw new MongoDbException(new StreamCorruptedException(
						"Unexpected operation read '" + opCode + "'."));
			}

			final Header header = new Header(length, requestId, responseId, op);
			Message message;
			switch (op) {
			case REPLY:
				message = new Reply(header, myBsonIn);
				break;
			case QUERY:
				message = new Query(header, myBsonIn);
				break;
			case UPDATE:
				message = new Update(myBsonIn);
				break;
			case INSERT:
				message = new Insert(header, myBsonIn);
				break;
			case GET_MORE:
				message = new GetMore(myBsonIn);
				break;
			case DELETE:
				message = new Delete(myBsonIn);
				break;
			case KILL_CURSORS:
				message = new KillCursors(myBsonIn);
				break;
			default:
				message = null;
				break;
			}

			return message;
		}

		catch (final IOException ioe) {
			final MongoDbException error = new ConnectionLostException(ioe);

			myTransport
					.shutdown(error, (ioe instanceof InterruptedIOException));

			throw error;
		}
	}
}