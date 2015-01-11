/*
 * #%L
 * TransportFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport;

import java.io.IOException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;

/**
 * TransportFactory provides the ability to create a new low level transport.
 *
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface TransportFactory {

    /**
     * Creates a new transport to connect to the address provided. Changes in
     * connection status of the transport and messages received by the transport
     * should be sent to the {@link TransportResponseListener}.
     *
     * @param server
     *            The {@link Server} to connect the transport to.
     * @param config
     *            The configuration for the driver.
     * @param encoderCache
     *            The cache for encoding strings.
     * @param decoderCache
     *            The cache for decoding strings.
     * @param responseListener
     *            The listener for changes in the state of the connect and
     *            messages received from the server.
     * @return The {@link Transport} to use in sending messages.
     * @throws IOException
     *             On a failure to open the transport.
     */
    public Transport<?> createTransport(Server server,
            MongoClientConfiguration config, StringEncoderCache encoderCache,
            StringDecoderCache decoderCache,
            TransportResponseListener responseListener) throws IOException;
}
