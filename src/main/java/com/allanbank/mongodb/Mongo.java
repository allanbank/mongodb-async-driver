/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Interface to bootstrap into interactions with MongoDB.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Mongo extends Closeable {

    /**
     * Returns the MongoDatabase with the specified name. This method does not
     * validate that the database already exists in the MongoDB instance.
     * 
     * @param name
     *            The name of the existing database.
     * @return The {@link MongoDatabase}.
     */
    public MongoDatabase getDatabase(String name);

    /**
     * Returns a future for the list of database names.
     * 
     * @return A {@link Future} for the list of available database names.
     */
    public Future<List<String>> listDatabases();
}
