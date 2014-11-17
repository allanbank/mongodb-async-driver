/*
 * #%L
 * BasicMongoClientMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.client.metrics.MongoClientMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.NoOpMongoMessageListener;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * BasicMongoClientMetrics provides the ability to accumulate basic metrics for
 * the client, databases, collections, servers, and operations.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicMongoClientMetrics extends AbstractMetrics implements
        MongoClientMetrics {

    /** The logger for the {@link BasicMongoClientMetrics}. */
    private static final Log LOG = LogFactory
            .getLog(BasicMongoClientMetrics.class);

    /**
     * The global message listener. This will never be null but will normally be
     * a {@link NoOpMongoMessageListener} that the JIT compiler can remove from
     * the code path.
     */
    private volatile MongoMessageListener myGlobalListener = NoOpMongoMessageListener.NO_OP;

    /** The listener to accumulate metrics for each database and collection. */
    private final BasicLogicalMetricsListener myLogicalMetrics;

    /** The listener to accumulate metrics for each type of operation. */
    private final BasicOperationMetricsListener myOperationMetrics;

    /** The listener to accumulate metrics for each server. */
    private final BasicServerMetricsListener myServerMetrics;

    /**
     * Creates a new BasicMongoClientMetrics.
     */
    public BasicMongoClientMetrics() {
        this(new BasicLogicalMetricsListener(),
                new BasicServerMetricsListener(),
                new BasicOperationMetricsListener());
    }

    /**
     * Creates a new BasicMongoClientMetrics.
     * 
     * @param logicalMetricsListener
     *            The logical metrics listener.
     * @param serverMetricsListener
     *            The server metrics listener.
     * @param operationMetricsListener
     *            The operation metrics listener.
     */
    protected BasicMongoClientMetrics(
            final BasicLogicalMetricsListener logicalMetricsListener,
            final BasicServerMetricsListener serverMetricsListener,
            final BasicOperationMetricsListener operationMetricsListener) {
        myLogicalMetrics = logicalMetricsListener;
        myServerMetrics = serverMetricsListener;
        myOperationMetrics = operationMetricsListener;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to remove all state.
     * </p>
     */
    @Override
    public void close() {
        myLogicalMetrics.close();
        myOperationMetrics.close();
        myServerMetrics.close();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing since the collection metrics are automatically
     * added to the global, logical and server metrics.
     * </p>
     */
    @Override
    public void connectionClosed(final MongoMessageListener listener) {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new {@link BasicConnectionMetrics}.
     * </p>
     */
    @Override
    public BasicConnectionMetrics newConnection(final String serverName) {
        return new BasicConnectionMetrics(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to collect metrics for the entire client.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        super.receive(serverName, messageId, sent, reply, latencyNanos);

        myLogicalMetrics.receive(serverName, messageId, sent, reply,
                latencyNanos);
        myOperationMetrics.receive(serverName, messageId, sent, reply,
                latencyNanos);
        myServerMetrics.receive(serverName, messageId, sent, reply,
                latencyNanos);
        myGlobalListener.receive(serverName, messageId, sent, reply,
                latencyNanos);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to collect metrics for the entire client.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        super.sent(serverName, messageId, sent);

        myLogicalMetrics.sent(serverName, messageId, sent);
        myOperationMetrics.sent(serverName, messageId, sent);
        myServerMetrics.sent(serverName, messageId, sent);
        myGlobalListener.sent(serverName, messageId, sent);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the listener as the global listener.
     * </p>
     */
    @Override
    public void setMessageListener(final MongoMessageListener listener) {
        if (listener != null) {
            LOG.warn(
                    "WARNING: Setting a global message listener should not be done in production: {}",
                    listener);
            myGlobalListener = listener;
        }
        else {
            myGlobalListener = NoOpMongoMessageListener.NO_OP;
        }
    }

    /**
     * Writes a human readable form of the server metrics.
     * 
     * @param writer
     *            The writer to write to.
     */
    @Override
    public void writeTo(final PrintWriter writer) {
        super.writeTo(writer, "Global", null);
        writer.println();
        myLogicalMetrics.writeTo(writer);
        writer.println();
        myOperationMetrics.writeTo(writer);
        writer.println();
        myServerMetrics.writeTo(writer);
        if (myGlobalListener != NoOpMongoMessageListener.NO_OP) {
            writer.println();
            writer.print("WARNING: Using global listener.");
        }
    }
}
