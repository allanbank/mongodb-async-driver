/*
 * #%L
 * OneThreadTransport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio.one;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.callback.NoOpCallback;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.AbstractSocketTransport;
import com.allanbank.mongodb.client.transport.bio.ReceiveRunnable;

/**
 * OneThreadTransport provides a handle for a socket connection.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class OneThreadTransport extends
        AbstractSocketTransport<OneThreadOutputBuffer> implements Receiver {

    /**
     * The buffers used each connection. Each buffer is shared by all
     * connections but there can be up to 1 buffer per application thread.
     */
    private final ThreadLocal<Reference<OneThreadOutputBuffer>> myBuffers;

    /** The cache for strings we write. */
    private final StringEncoderCache myEncoderCache;

    /** The thread receiving replies. */
    private final Thread myReceiver;

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
    public OneThreadTransport(Server server, MongoClientConfiguration config,
            StringEncoderCache encoderCache, StringDecoderCache decoderCache,
            TransportResponseListener responseListener,
            ThreadLocal<Reference<OneThreadOutputBuffer>> buffers)
            throws IOException {
        super(server, config, decoderCache, responseListener);

        myBuffers = buffers;
        myEncoderCache = encoderCache;

        myReceiveRunnable = new ReceiveRunnable(config, this);
        myReceiver = config.getThreadFactory().newThread(myReceiveRunnable);
        myReceiver.setDaemon(true);
        myReceiver.setName("MongoDB " + mySocket.getLocalPort() + "<--"
                + myServer.getCanonicalName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new {@link OneThreadOutputBuffer}.
     * </p>
     */
    @Override
    public OneThreadOutputBuffer createSendBuffer(int size) {
        final Reference<OneThreadOutputBuffer> bufferRef = myBuffers.get();
        OneThreadOutputBuffer buffer = (bufferRef != null) ? bufferRef.get()
                : null;
        if (buffer == null) {
            buffer = new OneThreadOutputBuffer(myEncoderCache);

            myBuffers.set(new SoftReference<OneThreadOutputBuffer>(buffer));
        }
        return buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (myOpen.compareAndSet(true, false)) {
            myReceiver.interrupt();

            // Now that output is shutdown. Close up the socket. This
            // Triggers the receiver to close if the interrupt didn't work.
            myOutput.close();
            myInput.close();
            mySocket.close();

            try {
                if (Thread.currentThread() != myReceiver) {
                    myReceiver.join();
                }
            }
            catch (final InterruptedException ie) {
                // Ignore.
            }

            myResponseListener
                    .closed(new MongoDbException("Connection closed."));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true since all send and receive tracking work is
     * done external to the transport.
     * </p>
     */
    @Override
    public boolean isIdle() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to add the buffered messages onto the send queue.
     * </p>
     */
    @Override
    public void send(OneThreadOutputBuffer buffer) throws IOException {
        try {
            buffer.writeTo(myOutput);
        }
        finally {
            buffer.clear();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link OneThreadOutputBuffer} that contains an
     * isMaster command to wake up the send thread.
     * </p>
     * 
     * @see AbstractSocketTransport#createIsMasterBuffer()
     */
    @Override
    protected OneThreadOutputBuffer createIsMasterBuffer() throws IOException {
        OneThreadOutputBuffer buffer = new OneThreadOutputBuffer(myEncoderCache);

        buffer.write(Integer.MAX_VALUE, new IsMaster(), new NoOpCallback());

        return buffer;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to start the receiver thread.
     * </p>
     * 
     * @see com.allanbank.mongodb.client.transport.Transport#start()
     */
    @Override
    public void start() {
        myReceiver.start();
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
            }
            catch (IOException ignore) {
                myLog.info("Error while flushing from the receive thread.",
                        ignore);
            }
            myReceiveRunnable.tryReceive();
        }
    }
}
