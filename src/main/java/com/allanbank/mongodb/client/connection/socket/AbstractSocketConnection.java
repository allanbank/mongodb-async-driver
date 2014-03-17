/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.SocketConnectionListener;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Header;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.ReplyHandler;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.error.DocumentToLargeException;
import com.allanbank.mongodb.error.ServerVersionException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * AbstractSocketConnection provides the basic functionality for a socket
 * connection that passes messages between the sender and receiver.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractSocketConnection implements Connection {

	/** The length of the message header in bytes. */
	public static final int HEADER_LENGTH = 16;

	/** The writer for BSON documents. Shares this objects {@link #myInput}. */
	protected final BsonInputStream myBsonIn;

	/** The writer for BSON documents. Shares this objects {@link #myOutput}. */
	protected final BufferingBsonOutputStream myBsonOut;

	/** Support for emitting property change events. */
	protected final PropertyChangeSupport myEventSupport;

	/** The executor for the responses. */
	protected final Executor myExecutor;

	/** The buffered input stream. */
	protected final InputStream myInput;

	/** The logger for the connection. */
	protected final Log myLog;

	/** Holds if the connection is open. */
	protected final AtomicBoolean myOpen;

	/** The buffered output stream. */
	protected final BufferedOutputStream myOutput;

	/** The queue of messages sent but waiting for a reply. */
	protected final PendingMessageQueue myPendingQueue;

	/** The open socket. */
	protected final Server myServer;

	/** Set to true when the connection should be gracefully closed. */
	protected final AtomicBoolean myShutdown;

	/** The open socket. */
	protected final Socket mySocket;

	/** The connections configuration. */
	private final MongoClientConfiguration myConfig;

	/** Tracks the number of sequential read timeouts. */
	private int myIdleTicks = 0;

	/** The {@link PendingMessage} used for the local cached copy. */
	private final PendingMessage myPendingMessage = new PendingMessage();

	/**
	 * Creates a new AbstractSocketConnection.
	 * 
	 * @param server
	 *            The MongoDB server to connect to.
	 * @param config
	 *            The configuration for the Connection to the MongoDB server.
	 * @throws SocketException
	 *             On a failure connecting to the MongoDB server.
	 * @throws IOException
	 *             On a failure to read or write data to the MongoDB server.
	 */
	public AbstractSocketConnection(final Server server,
			final MongoClientConfiguration config) throws SocketException,
			IOException {
		super();

		myServer = server;
		myConfig = config;

		myLog = LogFactory.getLog(getClass());

		myExecutor = config.getExecutor();
		myEventSupport = new PropertyChangeSupport(this);
		myOpen = new AtomicBoolean(false);
		myShutdown = new AtomicBoolean(false);

		mySocket = openSocket(server, config);
		updateSocketWithOptions(config);

		myOpen.set(true);

		myInput = mySocket.getInputStream();
		myBsonIn = new BsonInputStream(myInput);
		myBsonIn.setMaxCachedStringEntries(myConfig.getMaxCachedStringEntries());
		myBsonIn.setMaxCachedStringLength(myConfig.getMaxCachedStringLength());

		// Careful with the size of the buffer here. Seems Java likes to call
		// madvise(..., MADV_DONTNEED) for buffers over a certain size.
		// Net effect is that the performance of the system goes down the
		// drain. Some numbers using the
		// UnixDomainSocketAccepatanceTest.testMultiFetchiterator
		// 1M ==> More than a minute...
		// 512K ==> 24 seconds
		// 256K ==> 16.9 sec.
		// 128K ==> 17 sec.
		// 64K ==> 17 sec.
		// 32K ==> 16.5 sec.
		// Based on those numbers we set the buffer to 32K as larger does not
		// improve performance.
		myOutput = new BufferedOutputStream(mySocket.getOutputStream(),
				32 * 1024);
		myBsonOut = new BufferingBsonOutputStream(myOutput);
		myBsonOut.setMaxCachedStringEntries(myConfig
				.getMaxCachedStringEntries());
		myBsonOut.setMaxCachedStringLength(myConfig.getMaxCachedStringLength());

		myPendingQueue = new PendingMessageQueue(
				config.getMaxPendingOperationsPerConnection(),
				config.getLockType());
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to add the listener to this connection.
	 * </p>
	 */
	@Override
	public void addPropertyChangeListener(final PropertyChangeListener listener) {
		myEventSupport.addPropertyChangeListener(listener);
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
	public int getPendingCount() {
		return myPendingQueue.size();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to returns the server's name.
	 * </p>
	 */
	@Override
	public String getServerName() {
		return myServer.getCanonicalName();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * True if the connection is open and not shutting down.
	 * </p>
	 */
	@Override
	public boolean isAvailable() {
		return isOpen() && !isShuttingDown();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * True if the send and pending queues are empty.
	 * </p>
	 */
	@Override
	public boolean isIdle() {
		return myPendingQueue.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * True if the connection has not been closed.
	 * </p>
	 */
	@Override
	public boolean isOpen() {
		return myOpen.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShuttingDown() {
		return myShutdown.get();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Notifies the appropriate messages of the error.
	 * </p>
	 */
	@Override
	public void raiseErrors(final MongoDbException exception) {
		final PendingMessage message = new PendingMessage();

		while (myPendingQueue.poll(message)) {
			raiseError(exception, message.getReplyCallback());
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to remove the listener from this connection.
	 * </p>
	 */
	@Override
	public void removePropertyChangeListener(
			final PropertyChangeListener listener) {
		myEventSupport.removePropertyChangeListener(listener);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to mark the socket as shutting down and tickles the sender to
	 * make sure that happens as soon as possible.
	 * </p>
	 */
	@Override
	public void shutdown(final boolean force) {
		// Mark
		myShutdown.set(true);

		if (force) {
			IOUtils.close(this);
		} else {
			if (isOpen()) {
				// Force a message with a callback to wake the receiver up.
				send(new IsMaster(), new NoopCallback());
			}
		}
	}

	/**
	 * Starts the connection.
	 */
	public abstract void start();

	/**
	 * Stops the socket connection by calling {@link #shutdown(boolean)
	 * shutdown(false)}.
	 */
	public void stop() {
		shutdown(false);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return the socket information.
	 * </p>
	 */
	@Override
	public String toString() {
		return "MongoDB(" + mySocket.getLocalPort() + "-->"
				+ mySocket.getRemoteSocketAddress() + ")";
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Waits for the connections pending queues to empty.
	 * </p>
	 */
	@Override
	public void waitForClosed(final int timeout, final TimeUnit timeoutUnits) {
		long now = System.currentTimeMillis();
		final long deadline = now + timeoutUnits.toMillis(timeout);

		while (isOpen() && (now < deadline)) {
			try {
				// A slow spin loop.
				TimeUnit.MILLISECONDS.sleep(10);
			} catch (final InterruptedException e) {
				// Ignore.
				e.hashCode();
			}
			now = System.currentTimeMillis();
		}
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
				throw new MongoDbException("Unexpected operation read '"
						+ opCode + "'.");
			}

			final Header header = new Header(length, requestId, responseId, op);
			Message message = null;
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
			}

			myServer.incrementRepliesReceived();

			return message;
		}

		catch (final IOException ioe) {
			final MongoDbException error = new ConnectionLostException(ioe);

			shutdown(error, (ioe instanceof InterruptedIOException));

			throw error;
		}
	}

	/**
	 * Receives and process a single message.
	 */
	protected void doReceiveOne() {
		final Message received = doReceive();
		if (received instanceof Reply) {
			myIdleTicks = 0;
			final Reply reply = (Reply) received;
			final int replyId = reply.getResponseToId();
			boolean took = false;

			// Keep polling the pending queue until we get to
			// message based on a matching replyId.
			try {
				took = myPendingQueue.poll(myPendingMessage);
				while (took && (myPendingMessage.getMessageId() != replyId)) {

					final MongoDbException noReply = new MongoDbException(
							"No reply received.");

					// Note that this message will not get a reply.
					raiseError(noReply, myPendingMessage.getReplyCallback());

					// Keep looking.
					took = myPendingQueue.poll(myPendingMessage);
				}

				if (took) {
					// Must be the pending message's reply.
					reply(reply, myPendingMessage);
				} else {
					myLog.warn("Could not find the callback for reply '{}'.",
							replyId);
				}
			} finally {
				myPendingMessage.clear();
			}
		} else if (received != null) {
			myLog.warn("Received a non-Reply message: {}", received);
			shutdown(new ConnectionLostException(new StreamCorruptedException(
					"Received a non-Reply message: " + received)), false);
		} else {
			myIdleTicks += 1;
			if (myConfig.getMaxIdleTickCount() <= myIdleTicks) {
				// Shutdown the connection., nicely.
				shutdown(false);
			}
		}
	}

	/**
	 * Sends a single message to the connection.
	 * 
	 * @param messageId
	 *            The id to use for the message.
	 * @param message
	 *            The message to send.
	 * @throws IOException
	 *             On a failure sending the message.
	 */
	protected void doSend(final int messageId, final Message message)
			throws IOException {
		message.write(messageId, myBsonOut);
		myServer.incrementMessagesSent();
	}

	/**
	 * Updates to raise an error on the callback, if any.
	 * 
	 * @param exception
	 *            The thrown exception.
	 * @param replyCallback
	 *            The callback for the reply to the message.
	 */
	protected void raiseError(final Throwable exception,
			final Callback<Reply> replyCallback) {
		ReplyHandler.raiseError(exception, replyCallback, myExecutor);
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
					+ mySocket.getRemoteSocketAddress());
		}
		return result;
	}

	/**
	 * Updates to set the reply for the callback, if any.
	 * 
	 * @param reply
	 *            The reply.
	 * @param pendingMessage
	 *            The pending message.
	 */
	protected void reply(final Reply reply, final PendingMessage pendingMessage) {

		final long latency = pendingMessage.latency();

		// Add the latency.
		if (latency > 0) {
			myServer.updateAverageLatency(latency);
		}

		final Callback<Reply> callback = pendingMessage.getReplyCallback();
		ReplyHandler.reply(reply, callback, myExecutor);
	}

	/**
	 * Sends a single message.
	 * 
	 * @param pendingMessage
	 *            The message to be sent.
	 * 
	 * @throws InterruptedException
	 *             If the thread is interrupted waiting for a message to send.
	 * @throws IOException
	 *             On a failure sending the message.
	 */
	protected final void send(final PendingMessage pendingMessage)
			throws InterruptedException, IOException {
		final int messageId = pendingMessage.getMessageId();
		final Message message = pendingMessage.getMessage();

		// Mark the timestamp.
		pendingMessage.timestampNow();

		// Make sure the message is on the queue before the
		// message is sent to ensure the receive thread can
		// assume an empty pending queue means that there is
		// no message for the reply.
		if ((pendingMessage.getReplyCallback() != null)
				&& !myPendingQueue.offer(pendingMessage)) {
			// Push what we have out before blocking.
			flush();
			myPendingQueue.put(pendingMessage);
		}

		doSend(messageId, message);

		// If shutting down then flush after each message.
		if (myShutdown.get()) {
			flush();
		}
	}

	/**
	 * Shutsdown the connection on an error.
	 * 
	 * @param error
	 *            The error causing the shutdown.
	 * @param receiveError
	 *            If true then the socket experienced a receive error.
	 */
	protected void shutdown(final MongoDbException error,
			final boolean receiveError) {
		if (receiveError) {
			myServer.connectionTerminated();
		}

		// Have to assume all of the requests have failed that are pending.
		final PendingMessage message = new PendingMessage();
		while (myPendingQueue.poll(message)) {
			raiseError(error, message.getReplyCallback());
		}

		closeQuietly();
	}

	/**
	 * Updates the socket with the configuration's socket options.
	 * 
	 * @param config
	 *            The configuration to apply.
	 * @throws SocketException
	 *             On a failure setting the socket options.
	 */
	protected void updateSocketWithOptions(final MongoClientConfiguration config)
			throws SocketException {
		mySocket.setKeepAlive(config.isUsingSoKeepalive());
		mySocket.setSoTimeout(config.getReadTimeout());
		try {
			mySocket.setTcpNoDelay(true);
		} catch (final SocketException seIgnored) {
			// The junixsocket implementation does not support TCP_NO_DELAY,
			// which makes sense but it throws an exception instead of silently
			// ignoring - ignore it here.
			if (!"AFUNIXSocketException".equals(seIgnored.getClass()
					.getSimpleName())) {
				throw seIgnored;
			}
		}
		mySocket.setPerformancePreferences(1, 5, 6);
	}

	/**
	 * Ensures that the documents in the message do not exceed the maximum size
	 * allowed by MongoDB.
	 * 
	 * @param message1
	 *            The message to be sent to the server.
	 * @param message2
	 *            The second message to be sent to the server.
	 * @throws DocumentToLargeException
	 *             On a message being too large.
	 * @throws ServerVersionException
	 *             If one of the messages cannot be sent to the server version.
	 */
	protected void validate(final Message message1, final Message message2)
			throws DocumentToLargeException, ServerVersionException {

		final SizeOfVisitor visitor = new SizeOfVisitor();

		final Version serverVersion = myServer.getVersion();
		final int maxBsonSize = myServer.getMaxBsonObjectSize();

		message1.validateSize(visitor, maxBsonSize);
		validateVersion(message1, serverVersion);

		if (message2 != null) {
			visitor.reset();
			message2.validateSize(visitor, maxBsonSize);
			validateVersion(message1, serverVersion);
		}
	}

	/**
	 * Closes the connection to the server without allowing an exception to be
	 * thrown.
	 */
	private void closeQuietly() {
		try {
			close();
		} catch (final IOException e) {
			myLog.warn(e, "I/O exception trying to shutdown the connection.");
		}
	}

	/**
	 * Tries to open a connection to the server.
	 * 
	 * @param server
	 *            The server to open the connection to.
	 * @param config
	 *            The configuration for attempting to open the connection.
	 * @return The opened {@link Socket}.
	 * @throws IOException
	 *             On a failure opening a connection to the server.
	 */
	private Socket openSocket(final Server server,
			final MongoClientConfiguration config) throws IOException {
		final SocketFactory factory = config.getSocketFactory();

		IOException last = null;
		Socket socket = null;
		for (final InetSocketAddress address : myServer.getAddresses()) {
			try {

				socket = factory.createSocket();
				socket.connect(address, config.getConnectTimeout());

				// If the factory wants to know about the connection then let it
				// know first.
				if (factory instanceof SocketConnectionListener) {
					((SocketConnectionListener) factory).connected(address,
							socket);
				}

				// Let the server know the working connection.
				server.connectionOpened(address);

				last = null;
				break;
			} catch (final IOException error) {
				last = error;
				try {
					if (socket != null) {
						socket.close();
					}
				} catch (final IOException ignore) {
					myLog.info(
							"Could not close the defunct socket connection: {}",
							socket);
				}
			}

		}
		if (last != null) {
			server.connectFailed();
			throw last;
		}

		return socket;
	}

	/**
	 * Validates that the server we are about to send the message to knows how
	 * to handle the message.
	 * 
	 * @param message
	 *            The message to be sent.
	 * @param serverVersion
	 *            The server version.
	 * @throws ServerVersionException
	 *             If the messages cannot be sent to the server version.
	 */
	private void validateVersion(final Message message,
			final Version serverVersion) throws ServerVersionException {
		final Version messageVersion = message.getRequiredServerVersion();
		if ((messageVersion != null)
				&& (messageVersion.compareTo(serverVersion) > 0)) {

			throw new ServerVersionException(message.getOperationName(),
					messageVersion, serverVersion, message);
		}
	}

	/**
	 * NoopCallback provides a callback that does not look at the reply.
	 * 
	 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
	 */
	protected static final class NoopCallback implements Callback<Reply> {
		/**
		 * {@inheritDoc}
		 * <p>
		 * Overridden to do nothing.
		 * </p>
		 */
		@Override
		public void callback(final Reply result) {
			// Noop.
		}

		/**
		 * {@inheritDoc}
		 * <p>
		 * Overridden to do nothing.
		 * </p>
		 */
		@Override
		public void exception(final Throwable thrown) {
			// Noop.
		}
	}

}