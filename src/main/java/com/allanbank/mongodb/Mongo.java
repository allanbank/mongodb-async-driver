/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * Interface to bootstrap into interactions with MongoDB.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @deprecated Use the {@link MongoClient} interface instead. This interface
 *             will be removed on or after the 1.3.0 release.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public interface Mongo extends MongoClient {

    /**
     * Returns a Mongo instance that shares connections with this Mongo instance
     * but serializes all of its requests on a single connection.
     * <p>
     * While the returned Mongo instance is thread safe it is intended to be
     * used by a single logical thread to ensure requests issued to the MongoDB
     * server are guaranteed to be processed in the same order they are
     * requested.
     * </p>
     * <p>
     * Creation of the serial instance is lightweight with minimal object
     * allocation and no server interaction.
     * </p>
     *
     * @return Serialized view of the connections to the MongoDB Server.
     * @deprecated Use {@link MongoClient#asSerializedClient()} instead.
     */
    @Deprecated
    public Mongo asSerializedMongo();

    /**
     * Returns the configuration being used by the logical MongoDB connection.
     *
     * @return The configuration being used by the logical MongoDB connection.
     */
    @Override
    public MongoDbConfiguration getConfig();

}
