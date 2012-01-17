/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.io.IOException;
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
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClient implements Mongo {

    /** The client to interact with MongoDB. */
    private final MongoClientConnection myClient;

    /** The configuration for interacting with MongoDB. */
    private final MongoDbConfiguration myConfig;

    /**
     * Create a new MongoClient.
     * 
     * @param config
     *            The configuration for interacting with MongoDB.
     */
    public MongoClient(final MongoDbConfiguration config) {
        myConfig = config;
        myClient = new MongoClientConnection(myConfig);
    }

    @Override
    public void close() throws IOException {
        myClient.close();
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
        return new MongoDatabaseClient(myClient, name);
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
