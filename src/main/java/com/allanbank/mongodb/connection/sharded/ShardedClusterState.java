/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * ShardedClusterState provides an extension to the cluster state that
 * understands how to route requests to shards based on the query/document of
 * interest.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedClusterState extends ClusterState {

    /**
     * The URIs to pass to the shard servers as the configuration mongod
     * locations. This is determined as part of the cluster bootstrap so it
     * cannot be final. It should be set once.
     */
    private String myConfigDatabases;

    /** The state of the databases in the cluster. */
    private final ConcurrentMap<String, DatabaseShardState> myDatabases;

    /**
     * Creates a new ShardedClusterState.
     * 
     * @param config
     *            The configuration of this MongoDB client.
     */
    public ShardedClusterState(final MongoClientConfiguration config) {
        super(config);

        myConfigDatabases = null;
        myDatabases = new ConcurrentHashMap<String, DatabaseShardState>();
    }

    /**
     * Deletes the state of for the specified database/collection. This should
     * be called when a
     * {@link com.allanbank.mongodb.error.ShardConfigStaleException} is
     * encountered.
     * 
     * @param database
     *            The database that the query or document is going to be used
     *            with.
     * @param collection
     *            The collection that the query or document is going to be used
     *            with.
     */
    public void deleteState(final String database, final String collection) {

        // If we know about the database, then delegate to the
        // DatabaseShardState
        final DatabaseShardState dbState = myDatabases.get(database);
        if (dbState != null) {
            dbState.deleteState(collection);
        }
    }

    /**
     * Tries to determine the correct shard to receive the query/inserted
     * document.
     * 
     * @param database
     *            The database that the query or document is going to be used
     *            with.
     * @param collection
     *            The collection that the query or document is going to be used
     *            with.
     * @param queryOrDocument
     *            The query that will be used or the document that
     * @return The shard to use with the query/insert or <code>null</code> if a
     *         specific shard could not be determined.
     */
    public Shard findShardFor(final String database, final String collection,
            final Document queryOrDocument) {

        Shard result = null;

        // If we know about the database, then delegate to the
        // DatabaseShardState
        final DatabaseShardState dbState = myDatabases.get(database);
        if (dbState != null) {
            result = dbState.findShardFor(collection, queryOrDocument);
        }

        // ... otherwise TODO request the database information.
        // U... and use the mongos for now.

        return result;
    }

    /**
     * Returns the state of the databases within the cluster.
     * 
     * @return The state of the databases within the cluster.
     */
    public Map<String, DatabaseShardState> getCollections() {
        return Collections.unmodifiableMap(myDatabases);
    }

    /**
     * Returns the URIs to pass to the shard servers as the configuration mongod
     * locations.
     * 
     * @return The URIs to pass to the shard servers as the configuration mongod
     *         locations.
     */
    public String getConfigDatabases() {
        return myConfigDatabases;
    }

    /**
     * Returns the URIs to pass to the shard servers as the configuration mongod
     * locations.
     * 
     * @param configDatabases
     *            The URIs to pass to the shard servers as the configuration
     *            mongod locations.
     */
    public void setConfigDatabases(final String configDatabases) {
        myConfigDatabases = configDatabases;
    }

    /**
     * Sets the state of the databases within the cluster.
     * 
     * @param databases
     *            The state of the databases.
     */
    public void setDatabases(final Map<String, DatabaseShardState> databases) {
        myDatabases.clear();
        if (databases != null) {
            myDatabases.putAll(databases);
        }
    }
}
