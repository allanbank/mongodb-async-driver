/*
 * #%L
 * TwoThreadTransport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio.two;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.callback.NoOpCallback;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.PendingMessageQueue;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.AbstractSocketTransport;
import com.allanbank.mongodb.client.transport.bio.ReceiveRunnable;

/**
 * TwoThreadTransport provides a handle for a socket connection that uses a pair
 * of threads to read and write to the socket.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class TwoThreadTransport extends
		AbstractSocketTransport<TwoThreadOutputBuffer> implements Receiver {

	/**
	 * The buffers used each connection. Each buffer is shared by all
	 * connections but there can be up to 1 buffer per application thread.
	 */
	private final ThreadLocal<Reference<TwoThreadOutputBuffer>> myBuffers;

	/**
	 * The buffers used each connection. Each buffer is shared by all
	 * connections but there can be up to 1 buffer per application thread.
	 */
	private final PendingMessageQueue myToSendQueue;

	/** The writer for BSON documents. */
	private final BufferingBsonOutputStream myBsonOut;

	/** The thread receiving replies. */
	private final Thread myReceiver;

	/** The thread sending messages. */
	private final Thread mySender;

	/** The receiver for messages. */
	private final ReceiveRunnable myReceiveRunnable;

	/**
	 * Creates a new TwoThreadTransport.
	 * 
	 * @param server
	 *            The server to connect to.
	 * @param config
	 *            The clients configuration.
	 * @param encoderCache
	 *            The cache for the encoding of strings.
	 * @param decoderCache
	 *            The cache for the decoding of strings.
	 * @param responseListener
	 *            The listener for responses from the server.
	 * @param buffers
	 *            The per-thread transport buffers.
	 * @throws IOException
	 *             On a failure to create the connection to the server.
	 */
	public TwoThreadTransport(Server server, MongoClientConfiguration config,
			StringEncoderCache encoderCache, StringDecoderCache decoderCache,
			TransportResponseListener responseListener,
			ThreadLocal<Reference<TwoThreadOutputBuffer>> buffers)
			throws IOException {
		super(server, config, decoderCache, responseListener);

		myBuffers = buffers;

		myBsonOut = new BufferingBsonOutputStream(myOutput, encoderCache);

		myToSendQueue = new PendingMessageQueue(
				config.getMaxPendingOperationsPerConnection(),
				config.getLockType());

		int localPort = mySocket.getLocalPort();

		myReceiveRunnable = new ReceiveRunnable(config, this);
		myReceiver = config.getThreadFactory().newThread(myReceiveRunnable);
		myReceiver.setDaemon(true);
		myReceiver.setName("MongoDB " + localPort + "<--"
				+ myServer.getCanonicalName());

		mySender = config.getThreadFactory().newThread(new SendRunnable(this));
		mySender.setDaemon(true);
		mySender.setName("MongoDB " + localPort + "-->"
				+ myServer.getCanonicalName());
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to start the receiver and sender threads.
	 * </p>
	 * 
	 * @see Transport#start()
	 */
	@Override
	public void start() {
		myReceiver.start();
		mySender.start();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to create a {@link TwoThreadOutputBuffer} that contains an
	 * isMaster command to wake up the send thread.
	 * </p>
	 * 
	 * @see AbstractSocketTransport#createIsMasterBuffer()
	 */
	@Override
	protected TwoThreadOutputBuffer createIsMasterBuffer() {
		TwoThreadOutputBuffer buffer = new TwoThreadOutputBuffer();
		buffer.write(Integer.MAX_VALUE, new IsMaster(), new NoOpCallback());
		return buffer;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return a new {@link TwoThreadOutputBuffer}.
	 * </p>
	 */
	@Override
	public TwoThreadOutputBuffer createSendBuffer(int size) {
		final Reference<TwoThreadOutputBuffer> bufferRef = myBuffers.get();
		TwoThreadOutputBuffer buffer = (bufferRef != null) ? bufferRef.get()
				: null;
		if (buffer == null) {
			buffer = new TwoThreadOutputBuffer();

			myBuffers.set(new SoftReference<TwoThreadOutputBuffer>(buffer));
		}
		return buffer;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to add the buffered messages onto the send queue.
	 * </p>
	 */
	@Override
	public void send(TwoThreadOutputBuffer buffer)
			throws InterruptedIOException {
		try {
			for (PendingMessage toSend : buffer.getMessages()) {
				myToSendQueue.put(toSend);
			}
		} catch (InterruptedException e) {
			InterruptedIOException ioe = new InterruptedIOException(
					e.getMessage());

			ioe.initCause(e);

			throw ioe;
		} finally {
			buffer.clear();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close(MongoDbException error) throws IOException {
		if (myOpen.compareAndSet(true, false)) {

			mySender.interrupt();
			myReceiver.interrupt();

			try {
				if (Thread.currentThread() != mySender) {
					mySender.join();
				}
			} catch (final InterruptedException ie) {
				// Ignore.
			} finally {
				// Now that output is shutdown. Close up the socket. This
				// Triggers the receiver to close if the interrupt didn't work.
				myOutput.close();
				myInput.close();
				mySocket.close();
			}

			try {
				if (Thread.currentThread() != myReceiver) {
					myReceiver.join();
				}
			} catch (final InterruptedException ie) {
				// Ignore.
			}

			myResponseListener.closed(error);
		}
	}

	/**
	 * Returns the BSON output stream.
	 * 
	 * @return The BSON output stream.
	 */
	public BufferingBsonOutputStream getBsonOut() {
		return myBsonOut;
	}

	/**
	 * Returns the queue of message to be sent.
	 * 
	 * @return The queue of message to be sent.
	 */
	public PendingMessageQueue getToSendQueue() {
		return myToSendQueue;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * If currently on the receive thread then tries to perform a read.
	 * </p>
	 */
	@Override
	public void tryReceive() {
		if (Thread.currentThread() == myReceiver) {
			try {
				flush();
			} catch (IOException ignore) {
				myLog.info("Error while flushing from the receive thread.",
						ignore);
			}
			myReceiveRunnable.tryReceive();
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return true if the transport has any messages waiting to be
	 * sent.
	 * </p>
	 */
	@Override
	public boolean isIdle() {
		return myToSendQueue.isEmpty();
	}
}
