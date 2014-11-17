/*
 * #%L
 * BasicDatabaseMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.AbstractMetrics;

/**
 * BasicDatabaseMetrics provides the ability to accumulate metrics for a single
 * logical database.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicDatabaseMetrics extends AbstractMetrics {
    /** The metrics for each collection. */
    private final ConcurrentMap<String, BasicCollectionMetrics> myCollectionMetrics;

    /** The name of the database. */
    private final String myName;

    /**
     * Creates a new BasicDatabaseMetrics.
     * 
     * @param database
     *            The name of the database we are collecting metrics for.
     */
    public BasicDatabaseMetrics(final String database) {
        myName = database;
        myCollectionMetrics = new ConcurrentHashMap<String, BasicCollectionMetrics>();
    }

    /**
     * Removes all of the collection metrics.
     */
    @Override
    public void close() {
        super.close();
        for (final BasicCollectionMetrics metrics : myCollectionMetrics
                .values()) {
            metrics.close();
        }
        myCollectionMetrics.clear();
    }

    /**
     * Returns the map of collection names to the collection metrics.
     * 
     * @return The map of collection names to the collection metrics.
     */
    public Map<String, BasicCollectionMetrics> getCollectionMetrics() {
        return Collections.unmodifiableMap(myCollectionMetrics);
    }

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getName() {
        return myName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to accumulate the metrics for the database and then forward
     * the call to the appropriate {@link BasicCollectionMetrics}.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        super.receive(serverName, messageId, sent, reply, latencyNanos);

        final BasicCollectionMetrics metrics = findCollectionMetrics(sent);
        metrics.receive(serverName, messageId, sent, reply, latencyNanos);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to accumulate the metrics for the database and then forward
     * the call to the appropriate {@link BasicCollectionMetrics}.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        super.sent(serverName, messageId, sent);

        final BasicCollectionMetrics metrics = findCollectionMetrics(sent);
        metrics.sent(serverName, messageId, sent);

    }

    /**
     * Writes a human readable form of the database and collection metrics.
     * 
     * @param writer
     *            The writer to write to.
     */
    @Override
    public void writeTo(final PrintWriter writer) {
        writeTo(writer, "Database", getName());

        Set<String> names = new TreeSet<String>(myCollectionMetrics.keySet());
        for (final String name : names) {
            BasicCollectionMetrics collectionMetrics = myCollectionMetrics
                    .get(name);

            writer.println();
            collectionMetrics.writeTo(writer);
        }
    }

    /**
     * Extension point for derived classes to know when a collection has been
     * added.
     * 
     * @param metrics
     *            The metrics for the collection.
     */
    protected void addedCollection(final BasicCollectionMetrics metrics) {
        // Nothing - Extension point.
    }

    /**
     * Creates a a new {@link BasicCollectionMetrics} instance. This instance
     * may or may not be used.
     * 
     * @param collection
     *            The name of the collection to create a new metrics for.
     * @return The {@link BasicCollectionMetrics} for the database.
     */
    protected BasicCollectionMetrics createMetrics(final String collection) {
        return new BasicCollectionMetrics(collection);
    }

    /**
     * Locates or create the {@link BasicCollectionMetrics} for the message.
     * 
     * @param message
     *            The message to find the collection for.
     * @return The {@link BasicCollectionMetrics} for the message's collection.
     */
    private BasicCollectionMetrics findCollectionMetrics(final Message message) {
        final String collection = message.getCollectionName();
        BasicCollectionMetrics metrics = myCollectionMetrics.get(collection);
        if (metrics == null) {
            metrics = createMetrics(collection);
            final BasicCollectionMetrics existing = myCollectionMetrics
                    .putIfAbsent(collection, metrics);
            if (existing != null) {
                metrics = existing;
            }
            else {
                addedCollection(metrics);
            }
        }
        return metrics;
    }
}
