/*
 * #%L
 * TwoThreadTransportFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.lang.ref.Reference;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.TransportFactory;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * TwoThreadTransportFactory provides the {@link TransportFactory} that uses
 * blocking I/O sockets and a two or dual thread strategy. There is a single
 * reader thread and a single writer thread per socket.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TwoThreadTransportFactory implements TransportFactory {

	/**
	 * The buffers used each connection. Each buffer is shared by all
	 * connections but there can be up to 1 buffer per application thread.
	 */
	private final ThreadLocal<Reference<TwoThreadOutputBuffer>> myBuffers;

	/**
	 * Creates a new TwoThreadTransportFactory.
	 */
	public TwoThreadTransportFactory() {
		super();

		myBuffers = new ThreadLocal<Reference<TwoThreadOutputBuffer>>();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return a {@link TwoThreadTransport}.
	 * </p>
	 */
	@Override
	public TwoThreadTransport createTransport(Server server,
			MongoClientConfiguration config, StringEncoderCache encoderCache,
			StringDecoderCache decoderCache,
			TransportResponseListener responseListener) throws IOException {
		// Open the socket, setup the receive thread, setup the send thread and
		// queues between them.
		return new TwoThreadTransport(server, config, encoderCache,
				decoderCache, responseListener, myBuffers);
	}
}
