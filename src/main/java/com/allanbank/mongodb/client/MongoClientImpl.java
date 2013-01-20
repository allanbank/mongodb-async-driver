/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.List;

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
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientImpl implements MongoClient {

    /** The client to interact with MongoDB. */
    private final Client myClient;

    /**
     * Create a new MongoClient.
     * 
     * @param client
     *            The client interface for interacting with the database.
     */
    public MongoClientImpl(final Client client) {
        myClient = client;
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
     * Overridden to create the named database.
     * </p>
     * 
     * @see com.allanbank.mongodb.Mongo#getDatabase(java.lang.String)
     */
    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(myClient, name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a listDatabases command against the 'admin' database.
     * </p>
     * 
     * @see com.allanbank.mongodb.Mongo#listDatabaseNames()
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
