/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.List;
import java.util.concurrent.Future;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Interface for interacting with a MongoDB database. Primarily used to
 * {@link #getCollection(String) get} a {@link MongoCollection} .
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoDatabase {
    /** The name of the administration database. */
    public static final String ADMIN_NAME = "admin";

    /** The name of the configuration database for a sharded configuration. */
    public static final String CONFIG_NAME = "config";

    /** The name of the local database. */
    public static final String LOCAL_NAME = "local";

    /** The name of the test database. */
    public static final String TEST_NAME = "test";

    /**
     * Drops the database.
     * 
     * @return True if the database was successfully dropped, false otherwise.
     */
    public Future<Boolean> drop();

    /**
     * Returns the MongoCollection with the specified name. This method does not
     * validate that the collection already exists in the MongoDB database.
     * 
     * @param name
     *            The name of the collection.
     * @return The {@link MongoCollection}.
     */
    public MongoCollection getCollection(String name);

    /**
     * Returns the list of the collections contained within the database.
     * 
     * @return The list of the collections contained within the database.
     */
    public Future<List<String>> listCollections();

    /**
     * Runs a command against the database.
     * 
     * @param command
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     */
    public Future<Reply> runCommand(String command, Document options);
}
