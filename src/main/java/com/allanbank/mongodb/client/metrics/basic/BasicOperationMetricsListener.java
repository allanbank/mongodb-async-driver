/*
 * #%L
 * BasicOperationMetricsListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * BasicOperationMetricsListener accumulates the metrics on the different type
 * of operations (insert, update, delete, findAndModify, etc.).
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicOperationMetricsListener implements MongoMessageListener {

    /** The metrics for each operation type. */
    private final ConcurrentMap<String, BasicOperationMetrics> myOperationMetrics;

    /**
     * Creates a new BasicOperationMetricsListener.
     */
    public BasicOperationMetricsListener() {
        myOperationMetrics = new ConcurrentHashMap<String, BasicOperationMetrics>();
    }

    /**
     * Removes all of the operation type metrics.
     */
    public void close() {
        for (final BasicOperationMetrics metrics : myOperationMetrics.values()) {
            metrics.close();
        }
        myOperationMetrics.clear();
    }

    /**
     * Returns the map of operation metrics.
     * 
     * @return The map of operation metrics.
     */
    public Map<String, BasicOperationMetrics> getOperationMetrics() {
        return Collections.unmodifiableMap(myOperationMetrics);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicOperationMetrics}.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        final BasicOperationMetrics metrics = findOperationMetrics(sent
                .getOperationName());

        metrics.receive(serverName, messageId, sent, reply, latencyNanos);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicOperationMetrics}.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        final BasicOperationMetrics metrics = findOperationMetrics(sent
                .getOperationName());

        metrics.sent(serverName, messageId, sent);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the set of server metrics.
     * </p>
     */
    @Override
    public String toString() {
        final StringWriter sink = new StringWriter();
        final PrintWriter writer = new PrintWriter(sink);

        writeTo(writer);

        return sink.toString();
    }

    /**
     * Writes a human readable version of the server metrics to the writer.
     * 
     * @param writer
     *            The sink for the text.
     */
    public void writeTo(final PrintWriter writer) {
        boolean first = true;
        for (final BasicOperationMetrics serverMetrics : myOperationMetrics
                .values()) {
            if (first) {
                first = false;
            }
            else {
                writer.println();
            }
            writer.print(serverMetrics);
        }
    }

    /**
     * Extension point for derived classes to know when a operation type has
     * been added.
     * 
     * @param metrics
     *            The metrics for the server.
     */
    protected void addedOperation(final BasicOperationMetrics metrics) {
        // Nothing - Extension point.
    }

    /**
     * Creates a a new {@link BasicOperationMetrics} instance. This instance may
     * or may not be used.
     * 
     * @param operationName
     *            The name of the operation type to create a new metrics for.
     * @return The {@link BasicOperationMetrics} for the database.
     */
    protected BasicOperationMetrics createMetrics(final String operationName) {
        return new BasicOperationMetrics(operationName);
    }

    /**
     * Locates or create the {@link BasicOperationMetrics} for the message.
     * 
     * @param operationName
     *            The name of the operation for the message.
     * @return The {@link BasicOperationMetrics} for the message's operation.
     */
    protected BasicOperationMetrics findOperationMetrics(
            final String operationName) {
        BasicOperationMetrics metrics = myOperationMetrics.get(operationName);
        if (metrics == null) {
            metrics = new BasicOperationMetrics(operationName);
            final BasicOperationMetrics existing = myOperationMetrics
                    .putIfAbsent(operationName, metrics);
            if (existing != null) {
                metrics = existing;
            }
            else {
                addedOperation(metrics);
            }
        }
        return metrics;
    }
}
