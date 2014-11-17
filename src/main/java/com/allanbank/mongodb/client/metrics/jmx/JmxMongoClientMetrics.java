/*
 * #%L
 * JmxMongoClientMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.PrintWriter;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.basic.BasicConnectionMetrics;
import com.allanbank.mongodb.client.metrics.basic.BasicMongoClientMetrics;

/**
 * JmxMongoClientMetrics provides the global metrics for the client.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxMongoClientMetrics extends BasicMongoClientMetrics {

    /** The JmxSupport registering an unregistering MBeans. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxMongoClientMetrics.
     * 
     * @param support
     *            The MBean Server support to register all of the metrics with.
     */
    public JmxMongoClientMetrics(final JmxSupport support) {
        super(new JmxLogicalMetricsListener(support),
                new JmxServerMetricsListener(support),
                new JmxOperationMetricsListener(support));

        mySupport = support;
        mySupport.register(this, "MongoClient", Version.VERSION.toString());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to remove the MBean from JMX.
     * </p>
     */
    @Override
    public void close() {
        super.close();
        mySupport.unregister("MongoClient", Version.VERSION.toString());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link AbstractMetrics#close()} if the listener is an
     * implementation of {@link AbstractMetrics}.
     * </p>
     */
    @Override
    public void connectionClosed(final MongoMessageListener listener) {
        if (listener instanceof AbstractMetrics) {
            ((AbstractMetrics) listener).close();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new {@link BasicConnectionMetrics}.
     * </p>
     */
    @Override
    public JmxConnectionMetrics newConnection(final String serverName) {
        return new JmxConnectionMetrics(mySupport, serverName, this);
    }

    /**
     * Writes a human readable form of the server metrics.
     * 
     * @param writer
     *            The writer to write to.
     */
    @Override
    public void writeTo(final PrintWriter writer) {
        super.writeTo(writer);
        writer.println();
        writer.print("Note: Realtime metrics also available via JMX.");
    }
}
