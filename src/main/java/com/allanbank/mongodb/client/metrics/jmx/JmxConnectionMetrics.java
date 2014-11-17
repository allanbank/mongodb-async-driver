/*
 * #%L
 * JmxConnectionMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics.jmx;

import java.util.concurrent.atomic.AtomicInteger;

import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.basic.BasicConnectionMetrics;

/**
 * JmxConnectionMetrics provides metrics for a single connection to the server.
 * <p>
 * Unlike most of the other metrics, connection metrics are ephemeral.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxConnectionMetrics extends BasicConnectionMetrics {

    /** The next index for the connection. */
    private static final AtomicInteger ourNextIndex = new AtomicInteger();

    /** The index for the server. */
    private final int myIndex;

    /** The name of the server. */
    private final String myServerName;

    /** The JMX support class. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxConnectionMetrics.
     * 
     * @param support
     *            JMX support class.
     * @param serverName
     *            The name of the server for the collection.
     * @param listener
     *            The parent listener for the messages.
     */
    public JmxConnectionMetrics(final JmxSupport support,
            final String serverName, final MongoMessageListener listener) {
        super(listener);

        mySupport = support;
        myServerName = serverName;
        myIndex = ourNextIndex.incrementAndGet();

        mySupport.register(this, "connection", myServerName, myIndex);
    }

    /**
     * Removes the connection from JMX.
     */
    @Override
    public void close() {
        super.close();
        mySupport.unregister("connection", myServerName, myIndex);
    }
}