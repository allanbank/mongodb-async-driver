/*
 * #%L
 * BasicLogicalMetricsListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * BasicLogicalMetricsListener accumulates the metrics on the messages sent to
 * the logical database and collections.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicLogicalMetricsListener implements MongoMessageListener {

    /** The metrics for each database. */
    private final ConcurrentMap<String, BasicDatabaseMetrics> myDatabaseMetrics;

    /**
     * Creates a new BasicLogicalMetricsListener.
     */
    public BasicLogicalMetricsListener() {
        myDatabaseMetrics = new ConcurrentHashMap<String, BasicDatabaseMetrics>();
    }

    /**
     * Removes all of the database metrics.
     */
    public void close() {
        for (final BasicDatabaseMetrics metrics : myDatabaseMetrics.values()) {
            metrics.close();
        }
        myDatabaseMetrics.clear();
    }

    /**
     * Returns the map of database metrics.
     * 
     * @return The map of database metrics.
     */
    public Map<String, BasicDatabaseMetrics> getDatabaseMetrics() {
        return Collections.unmodifiableMap(myDatabaseMetrics);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicDatabaseMetrics}.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        final BasicDatabaseMetrics metrics = findDatabaseMetrics(sent);

        metrics.receive(serverName, messageId, sent, reply, latencyNanos);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the call to the appropriate
     * {@link BasicDatabaseMetrics}.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        final BasicDatabaseMetrics metrics = findDatabaseMetrics(sent);

        metrics.sent(serverName, messageId, sent);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the set of logical metrics.
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
     * Writes a human readable version of the logical metrics to the writer.
     * 
     * @param writer
     *            The sink for the text.
     */
    public void writeTo(final PrintWriter writer) {
        boolean first = true;
        final Set<String> names = new TreeSet<String>(
                myDatabaseMetrics.keySet());
        for (final String name : names) {
            final BasicDatabaseMetrics dbMetrics = myDatabaseMetrics.get(name);

            if (first) {
                first = false;
            }
            else {
                writer.println();
            }
            dbMetrics.writeTo(writer);
        }
    }

    /**
     * Extension point for derived classes to know when a database has been
     * added.
     * 
     * @param metrics
     *            The metrics for the database.
     */
    protected void addedDatabase(final BasicDatabaseMetrics metrics) {
        // Nothing - Extension point.
    }

    /**
     * Creates a a new {@link BasicDatabaseMetrics} instance. This instance may
     * or may not be used.
     * 
     * @param database
     *            The name of the database to create a new metrics for.
     * @return The {@link BasicDatabaseMetrics} for the database.
     */
    protected BasicDatabaseMetrics createMetrics(final String database) {
        return new BasicDatabaseMetrics(database);
    }

    /**
     * Locates or create the {@link BasicDatabaseMetrics} for the message.
     * 
     * @param message
     *            The message to find the database for.
     * @return The {@link BasicDatabaseMetrics} for the message's database.
     */
    private BasicDatabaseMetrics findDatabaseMetrics(final Message message) {
        final String database = message.getDatabaseName();
        BasicDatabaseMetrics metrics = myDatabaseMetrics.get(database);
        if (metrics == null) {
            metrics = createMetrics(database);
            final BasicDatabaseMetrics existing = myDatabaseMetrics
                    .putIfAbsent(database, metrics);
            if (existing != null) {
                metrics = existing;
            }
            else {
                addedDatabase(metrics);
            }
        }
        return metrics;
    }
}
