/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Interface for interacting with a MongoDB database. Primarily used to
 * {@link #getCollection(String) get} a {@link MongoCollection} .
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoDatabase {
    /** The name of the administration database. */
    public static final String ADMIN_NAME = MongoClientConfiguration.ADMIN_DB_NAME;

    /** The name of the configuration database for a sharded configuration. */
    public static final String CONFIG_NAME = "config";

    /** The name of the local database. */
    public static final String LOCAL_NAME = "local";

    /** The name of the test database. */
    public static final String TEST_NAME = "test";

    /**
     * Creates the capped collection with the specified name and size on the
     * server.
     * 
     * @param name
     *            The name of the collection.
     * @param size
     *            The size of the collection in bytes.
     * @return True if the collection was created, false otherwise (including it
     *         already exists).
     * @throws MongoDbException
     *             On a failure to create the collection.
     */
    public boolean createCappedCollection(String name, long size)
            throws MongoDbException;

    /**
     * Creates the collection with the specified name on the server.
     * 
     * @param name
     *            The name of the collection.
     * @param options
     *            The options for the collection being created.
     * @return True if the collection was created, false otherwise (including it
     *         already exists).
     * @throws MongoDbException
     *             On a failure to create the collection.
     */
    public boolean createCollection(String name, DocumentAssignable options)
            throws MongoDbException;

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
     * Returns the durability for write operations sent to the server from this
     * {@link MongoDatabase} instance.
     * <p>
     * Defaults to the {@link Durability} from the {@link Mongo}'s
     * configuration.
     * </p>
     * 
     * @return The durability for write operations on the server.
     * 
     * @see MongoClientConfiguration#getDefaultDurability()
     */
    public Durability getDurability();

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getName();

    /**
     * Retrieves the profiling level for the database.
     * 
     * @return The current profiling level.
     * @throws MongoDbException
     *             On a failure to create the collection.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/profile/">profile
     *      Command Reference</a>
     */
    public ProfilingStatus getProfilingStatus() throws MongoDbException;

    /**
     * Returns the read preference for queries from this {@link MongoDatabase}
     * instance.
     * <p>
     * Defaults to {@link ReadPreference} from the {@link Mongo}'s
     * configuration.
     * </p>
     * 
     * @return The default read preference for a query.
     * 
     * @see MongoClientConfiguration#getDefaultReadPreference()
     */
    public ReadPreference getReadPreference();

    /**
     * Returns the list of the collections contained within the database.
     * 
     * @return The list of the collections contained within the database.
     * @throws MongoDbException
     *             On an error listing the collections.
     */
    public List<String> listCollectionNames() throws MongoDbException;

    /**
     * Returns the list of the collections contained within the database.
     * 
     * @return The list of the collections contained within the database.
     * @throws MongoDbException
     *             On an error listing the collections.
     * @deprecated Use the {@link #listCollectionNames()} method instead.
     */
    @Deprecated
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
     *            The command document to run.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public Document runCommand(DocumentAssignable command)
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
    public Document runCommand(String commandName, int commandValue,
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
    public Document runCommand(String commandName, String commandValue,
            DocumentAssignable options) throws MongoDbException;

    /**
     * Runs a command against the database.
     * 
     * @param reply
     *            {@link Callback} that will be notified of the command results.
     * @param command
     *            The command document to run.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public void runCommandAsync(Callback<Document> reply,
            DocumentAssignable command) throws MongoDbException;

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
            int commandValue, DocumentAssignable options)
            throws MongoDbException;

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
     *            The command document to run.
     * @return The result of the command.
     * @throws MongoDbException
     *             On an error issuing the command or in running the command
     */
    public ListenableFuture<Document> runCommandAsync(DocumentAssignable command)
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
    public ListenableFuture<Document> runCommandAsync(String command)
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
    public ListenableFuture<Document> runCommandAsync(String command,
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
    public ListenableFuture<Document> runCommandAsync(String commandName,
            int commandValue, DocumentAssignable options)
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
    public ListenableFuture<Document> runCommandAsync(String commandName,
            String commandValue, DocumentAssignable options)
            throws MongoDbException;

    /**
     * Sets the durability for write operations from this {@link MongoDatabase}
     * instance.
     * <p>
     * Defaults to the {@link Durability} from the {@link Mongo}'s configuration
     * if set to <code>null</code>.
     * </p>
     * 
     * @param durability
     *            The durability for write operations on the server.
     * 
     * @see MongoClientConfiguration#getDefaultDurability()
     */
    public void setDurability(final Durability durability);

    /**
     * Sets the profiling level for the database.
     * 
     * @param profileLevel
     *            The desired profiling level
     * @return True if the profiling level was changed. Note if the level
     *         provided matches the profiling level already set then this method
     *         returns <code>false</code>.
     * @throws MongoDbException
     *             On a failure to create the collection.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/profile/">profile
     *      Command Reference</a>
     */
    public boolean setProfilingStatus(ProfilingStatus profileLevel)
            throws MongoDbException;

    /**
     * Sets the value of the read preference for a queries from this
     * {@link MongoDatabase} instance.
     * <p>
     * Defaults to the {@link ReadPreference} from the {@link Mongo}'s
     * configuration if set to <code>null</code>.
     * </p>
     * 
     * @param readPreference
     *            The read preference for a query.
     * 
     * @see MongoClientConfiguration#getDefaultReadPreference()
     */
    public void setReadPreference(final ReadPreference readPreference);

    /**
     * Returns the statistics for the database.
     * 
     * @return The results document with the database statistics.
     * @throws MongoDbException
     *             On an error collecting the database statistics.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/dbStats/">dbStats
     *      Command Reference</a>
     */
    public Document stats() throws MongoDbException;
}
