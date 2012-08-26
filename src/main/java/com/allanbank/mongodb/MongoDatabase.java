/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.List;
import java.util.concurrent.Future;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Interface for interacting with a MongoDB database. Primarily used to
 * {@link #getCollection(String) get} a {@link MongoCollection} .
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoDatabase {
    /** The name of the administration database. */
    public static final String ADMIN_NAME = MongoDbConfiguration.ADMIN_DB_NAME;

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
     * @throws MongoDbException
     *             On an error issuing the drop command or in running the
     *             command
     */
    public boolean drop() throws MongoDbException;

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
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getName();

    /**
     * Returns the list of the collections contained within the database.
     * 
     * @return The list of the collections contained within the database.
     * @throws MongoDbException
     *             On an error listing the collections.
     */
    public List<String> listCollections() throws MongoDbException;

    /**
     * Runs an administrative command against the 'admin' database.
     * 
     * @param command
     *            The name of the command to run.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runAdminCommand(String command) throws MongoDbException;

    /**
     * Runs an administrative command against the 'admin' database.
     * 
     * @param command
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runAdminCommand(String command, DocumentAssignable options)
            throws MongoDbException;

    /**
     * Runs an administrative command against the 'admin' database.
     * 
     * @param commandName
     *            The name of the command to run.
     * @param commandValue
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runAdminCommand(String commandName, String commandValue,
            DocumentAssignable options) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param command
     *            The name of the command to run.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runCommand(String command) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param command
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runCommand(String command, DocumentAssignable options)
            throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param commandName
     *            The name of the command to run.
     * @param commandValue
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runCommand(String commandName, String commandValue,
            DocumentAssignable options) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param reply
     *            {@link Callback} that will be notified of the command results.
     * @param command
     *            The name of the command to run.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public void runCommandAsync(Callback<Document> reply, String command)
            throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param reply
     *            {@link Callback} that will be notified of the command results.
     * @param command
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public void runCommandAsync(Callback<Document> reply, String command,
            DocumentAssignable options) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param reply
     *            {@link Callback} that will be notified of the command results.
     * @param commandName
     *            The name of the command to run.
     * @param commandValue
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public void runCommandAsync(Callback<Document> reply, String commandName,
            String commandValue, DocumentAssignable options)
            throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param command
     *            The name of the command to run.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Future<Document> runCommandAsync(String command)
            throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param command
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Future<Document> runCommandAsync(String command,
            DocumentAssignable options) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param commandName
     *            The name of the command to run.
     * @param commandValue
     *            The name of the command to run.
     * @param options
     *            Optional (may be null) options for the command.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Future<Document> runCommandAsync(String commandName,
            String commandValue, DocumentAssignable options)
            throws MongoDbException;

}
