/*
 * #%L
 * ConnectionModel.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import com.allanbank.mongodb.client.transport.TransportFactory;
import com.allanbank.mongodb.client.transport.bio.one.OneThreadTransportFactory;
import com.allanbank.mongodb.client.transport.bio.two.TwoThreadTransportFactory;

/**
 * ConnectionModel provides an enumeration of the connection models that the
 * driver supports. Currently this is related to the number of threads used by
 * the socket connections to the server.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ConnectionModel {

    /**
     * Each sender thread writes the message to the socket connection directly.
     * A single receive thread is used per connection to receive the replies.
     * While a sender writes each message to the socket the driver still has the
     * ability to batch multiple send requests in a packet but batching is
     * limited to concurrent senders. A single threaded application will not
     * batch requests.
     * <p>
     * This is the default {@code ConnectionModel}.
     * </p>
     * <p>
     * In a multi-threaded application this {@code ConnectionModel} should
     * perform as well as if not better than the {@link #SENDER_RECEIVER_THREAD}
     * model. This is achieved by completely avoiding cross thread passing of
     * the message on a send.
     * </p>
     */
    RECEIVER_THREAD(new OneThreadTransportFactory()),

    /**
     * Each socket uses a pair of threads: sender and receiver.
     * <p>
     * This was the default {@code ConnectionModel} in versions prior to 1.3.0.
     * </p>
     * <p>
     * This {@code ConnectionModel} is most useful for connections where a
     * single {@code write()} to the socket implementation is slow and the
     * application only uses a single write thread.
     * </p>
     * 
     * @since 1.0.0
     */
    SENDER_RECEIVER_THREAD(new TwoThreadTransportFactory());

    /** The {@link TransportFactory} implementing the connection model. */
    private transient final TransportFactory myFactory;

    /**
     * Creates a new ConnectionModel.
     * 
     * @param factory
     *            The {@link TransportFactory} implementing the connection
     *            model.
     */
    private ConnectionModel(TransportFactory factory) {
        myFactory = factory;
    }

    /**
     * Returns the {@link TransportFactory} implementing the connection model.
     * 
     * @return The {@link TransportFactory} implementing the connection model.
     */
    public TransportFactory getFactory() {
        return myFactory;
    }
}
