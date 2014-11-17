/*
 * #%L
 * BasicServerMetricsListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * BasicServerMetricsListener accumulates the metrics on the messages sent to a
 * single server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicServerMetricsListener implements MongoMessageListener {

    /** The metrics for each server. */
    private final ConcurrentMap<String, BasicServerMetrics> myServerMetrics;

    /**
     * Creates a new BasicServerMetricsListener.
     */
    public BasicServerMetricsListener() {
        myServerMetrics = new ConcurrentHashMap<String, BasicServerMetrics>();
    }

    /**
     * Removes all of the database metrics.
     */
    public void close() {
        for (final BasicServerMetrics metrics : myServerMetrics.values()) {
            metrics.close();
        }
        myServerMetrics.clear();
    }

    /**
     * Returns the map of server metrics.
     * 
     * @return The map of server metrics.
     */
    public Map<String, BasicServerMetrics> getServerMetrics() {
        return Collections.unmodifiableMap(myServerMetrics);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicServerMetrics}.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        final BasicServerMetrics metrics = findServerMetrics(serverName);

        metrics.receive(serverName, messageId, sent, reply, latencyNanos);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicServerMetrics}.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        final BasicServerMetrics metrics = findServerMetrics(serverName);

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
        Set<String> names = new TreeSet<String>(myServerMetrics.keySet());
        for (final String name : names) {
            BasicServerMetrics serverMetrics = myServerMetrics.get(name);
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
     * Extension point for derived classes to know when a server has been added.
     * 
     * @param metrics
     *            The metrics for the server.
     */
    protected void addedServer(final BasicServerMetrics metrics) {
        // Nothing - Extension point.
    }

    /**
     * Creates a a new {@link BasicServerMetrics} instance. This instance may or
     * may not be used.
     * 
     * @param serverName
     *            The name of the server to create a new metrics for.
     * @return The {@link BasicServerMetrics} for the database.
     */
    protected BasicServerMetrics createMetrics(final String serverName) {
        return new BasicServerMetrics(serverName);
    }

    /**
     * Locates or create the {@link BasicServerMetrics} for the message.
     * 
     * @param serverName
     *            The name of the server that the message was sent to or
     *            received from.
     * @return The {@link BasicServerMetrics} for the message's server.
     */
    protected BasicServerMetrics findServerMetrics(final String serverName) {
        BasicServerMetrics metrics = myServerMetrics.get(serverName);
        if (metrics == null) {
            metrics = new BasicServerMetrics(serverName);
            final BasicServerMetrics existing = myServerMetrics.putIfAbsent(
                    serverName, metrics);
            if (existing != null) {
                metrics = existing;
            }
            else {
                addedServer(metrics);
            }
        }
        return metrics;
    }
}
