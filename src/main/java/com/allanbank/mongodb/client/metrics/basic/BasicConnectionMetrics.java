/*
 * #%L
 * BasicConnectionMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics.basic;

import java.io.PrintWriter;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.client.metrics.ConnectionMetricsCollector;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * BasicConnectionMetrics provides for collecting metrics for a single
 * connection.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicConnectionMetrics
        extends AbstractMetrics
        implements ConnectionMetricsCollector {

    /** The parent listener for the messages. */
    private final MongoMessageListener myParentListener;

    /**
     * Creates a new BasicConnectionMetrics.
     *
     * @param listener
     *            The parent listener for the messages.
     */
    public BasicConnectionMetrics(final MongoMessageListener listener) {
        myParentListener = listener;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to collect metrics for the connection and then delegate to the
     * parent listener.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        super.receive(serverName, messageId, sent, reply, latencyNanos);

        if (sent != null) {
            myParentListener.receive(serverName, messageId, sent, reply,
                    latencyNanos);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to collect metrics for the connection and then delegate to the
     * parent listener.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        super.sent(serverName, messageId, sent);

        myParentListener.sent(serverName, messageId, sent);
    }

    /**
     * Writes a human readable form of the collection metrics.
     *
     * @param writer
     *            The writer to write to.
     */
    @Override
    public void writeTo(final PrintWriter writer) {
        // Use the identity hash code as a stable identifier.
        writeTo(writer, "Connection",
                String.valueOf(System.identityHashCode(this)));
    }
}
