/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Closeable;
import java.util.List;

/**
 * Interface to bootstrap into interactions with MongoDB.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoClient extends Closeable {

    /**
     * Returns a MongoClient instance that shares connections with this
     * MongoClient instance but serializes all of its requests on a single
     * connection.
     * <p>
     * While the returned MongoClient instance is thread safe it is intended to
     * be used by a single logical thread to ensure requests issued to the
     * MongoDB server are guaranteed to be processed in the same order they are
     * requested.
     * </p>
     * <p>
     * Creation of the serial instance is lightweight with minimal object
     * allocation and no server interaction.
     * </p>
     * 
     * @return A list of available database names.
     */
    public MongoClient asSerializedClient();

    /**
     * Returns the configuration being used by the logical MongoDB connection.
     * 
     * @return The configuration being used by the logical MongoDB connection.
     */
    public MongoClientConfiguration getConfig();

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
     * @return A list of available database names.
     */
    public List<String> listDatabases();
}
