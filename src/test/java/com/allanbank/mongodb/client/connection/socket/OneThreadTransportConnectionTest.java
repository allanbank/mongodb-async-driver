/*
 * #%L
 * AbstractTransportConnectionTestCases.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.socket;

import java.io.IOException;
import java.net.SocketException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportFactory;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;
import com.allanbank.mongodb.client.transport.bio.one.OneThreadTransportFactory;

/**
 * AbstractTransportConnectionTestCases provides tests for the
 * {@link TransportConnection} class when using the
 * {@link OneThreadTransportFactory}.
 *
 * @copyright 2011-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class OneThreadTransportConnectionTest
        extends AbstractTransportConnectionTestCases {

    /**
     * Creates the {@link TransportConnection} using a
     * {@link OneThreadTransportFactory}.
     *
     * @param server
     *            The server to connect to.
     * @param config
     *            The configuration for the connection.
     * @throws SocketException
     *             On a failure connecting.
     * @throws IOException
     *             On a failure talking to the server.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void connect(final Server server,
            final MongoClientConfiguration config) throws SocketException,
            IOException {
        myTestConnection = new TransportConnection(server, config, myCollector);

        final TransportFactory factory = new OneThreadTransportFactory();
        final Transport<TransportOutputBuffer> transport = (Transport<TransportOutputBuffer>) factory
                .createTransport(server, config, new StringEncoderCache(),
                        new StringDecoderCache(), myTestConnection);

        myTestConnection.setTransport(transport);
        myTestConnection.start();
    }
}
