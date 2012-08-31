/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.Mongo;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * Implements the bootstrap point for interactions with MongoDB.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoImpl implements Mongo {

    /** The client to interact with MongoDB. */
    private final Client myClient;

    /**
     * Create a new MongoClient.
     * 
     * @param client
     *            The client interface for interacting with the database.
     */
    public MongoImpl(final Client client) {
        myClient = client;
    }

    /**
     * Create a new MongoClient.
     * 
     * @param config
     *            The configuration for interacting with MongoDB.
     */
    public MongoImpl(final MongoDbConfiguration config) {
        this(new ClientImpl(config));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a new Mongo instance around a SerialClientImpl.
     * </p>
     */
    @Override
    public Mongo asSerializedMongo() {
        if (myClient instanceof SerialClientImpl) {
            return this;
        }

        return new MongoImpl(new SerialClientImpl((ClientImpl) myClient));
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
    public MongoDbConfiguration getConfig() {
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
     * @see com.allanbank.mongodb.Mongo#listDatabases()
     */
    @Override
    public List<String> listDatabases() {

        final MongoDatabase db = getDatabase("admin");
        final Document result = db.runAdminCommand("listDatabases");

        final List<String> names = new ArrayList<String>();
        for (final StringElement nameElement : result.queryPath(
                StringElement.class, "databases", ".*", "name")) {

            names.add(nameElement.getValue());
        }

        return names;
    }
}
