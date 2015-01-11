/*
 * #%L
 * MongoClientImpl.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * Implements the bootstrap point for interactions with MongoDB.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientImpl
        implements MongoClient {

    /** The client to interact with MongoDB. */
    private final Client myClient;

    /** The set of databases in use. */
    private final ConcurrentMap<String, Reference<MongoDatabase>> myDatabases;

    /** The queue of references to the databases that have been reclaimed. */
    private final ReferenceQueue<MongoDatabase> myReferenceQueue = new ReferenceQueue<MongoDatabase>();

    /**
     * Create a new MongoClient.
     *
     * @param client
     *            The client interface for interacting with the database.
     */
    public MongoClientImpl(final Client client) {
        myClient = client;

        myDatabases = new ConcurrentHashMap<String, Reference<MongoDatabase>>();
    }

    /**
     * Create a new MongoClient.
     *
     * @param config
     *            The configuration for interacting with MongoDB.
     */
    public MongoClientImpl(final MongoClientConfiguration config) {
        this(new ClientImpl(config));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a new Mongo instance around a SerialClientImpl.
     * </p>
     */
    @Override
    public MongoClient asSerializedClient() {
        if (myClient instanceof SerialClientImpl) {
            return this;
        }

        return new MongoClientImpl(new SerialClientImpl((ClientImpl) myClient));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the underlying client.
     * </p>
     */
    @Override
    public void close() {
        myClient.close();
    }

    /**
     * Returns the client value.
     *
     * @return The client value.
     */
    public Client getClient() {
        return myClient;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the clients configuration.
     * </p>
     */
    @Override
    public MongoClientConfiguration getConfig() {
        return myClient.getConfig();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link MongoDatabase} instance with the given
     * name.
     * </p>
     *
     * @see MongoClient#getDatabase(String)
     */
    @Override
    public MongoDatabase getDatabase(final String name) {
        MongoDatabase database = null;
        Reference<MongoDatabase> ref = myDatabases.get(name);
        if (ref != null) {
            database = ref.get();
            if (database == null) {
                // Reference was taken while in the map. Remove it from the map.
                myDatabases.remove(name, ref);
            }
        }

        // Create a new one.
        if (database == null) {
            database = new MongoDatabaseImpl(this, myClient, name);
            ref = new NamedReference<MongoDatabase>(name, database,
                    myReferenceQueue);

            final Reference<MongoDatabase> existing = myDatabases.putIfAbsent(
                    name, ref);
            if (existing != null) {
                final MongoDatabase existingDb = existing.get();
                if (existingDb != null) {
                    database = existingDb;
                }
                // else ... Extremely unlikely but if the reference came and
                // went that quick it is the next guys problem to add one. We
                // will return the one we created.
            }
        }

        // Clean out any garbage collected references.
        Reference<?> polled;
        while ((polled = myReferenceQueue.poll()) != null) {
            if (polled instanceof NamedReference) {
                myDatabases.remove(((NamedReference<?>) polled).getName(),
                        polled);
            }
        }

        return database;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a listDatabases command against the 'admin' database.
     * </p>
     *
     * @see MongoClient#listDatabaseNames()
     */
    @Override
    public List<String> listDatabaseNames() {

        final MongoDatabase db = getDatabase("admin");
        final Document result = db.runAdminCommand("listDatabases");

        final List<String> names = new ArrayList<String>();
        for (final StringElement nameElement : result.find(StringElement.class,
                "databases", ".*", "name")) {

            names.add(nameElement.getValue());
        }

        return names;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public List<String> listDatabases() {
        return listDatabaseNames();
    }

    /**
     * Restarts an iterator that was previously saved.
     *
     * @param cursorDocument
     *            The document containing the state of the cursor.
     * @return The restarted iterator.
     * @throws IllegalArgumentException
     *             If the document does not contain a valid cursor state.
     */
    @Override
    public MongoIterator<Document> restart(
            final DocumentAssignable cursorDocument)
            throws IllegalArgumentException {
        return myClient.restart(cursorDocument);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #restart(StreamCallback, DocumentAssignable)}
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl restart(final LambdaCallback<Document> results,
            final DocumentAssignable cursorDocument)
            throws IllegalArgumentException {
        return restart(new LambdaCallbackAdapter<Document>(results),
                cursorDocument);
    }

    /**
     * Restarts a document stream from a cursor that was previously saved.
     *
     * @param results
     *            Callback that will be notified of the results of the cursor.
     * @param cursorDocument
     *            The document containing the state of the cursor.
     * @throws IllegalArgumentException
     *             If the document does not contain a valid cursor state.
     */
    @Override
    public MongoCursorControl restart(final StreamCallback<Document> results,
            final DocumentAssignable cursorDocument)
            throws IllegalArgumentException {
        return myClient.restart(results, cursorDocument);
    }
}
