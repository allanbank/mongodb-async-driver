/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.bson.Document;

/**
 * DatabaseShardState provides the state of the database with respect to
 * sharding.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DatabaseShardState {

    /** The mapping of collection names to their states. */
    private final ConcurrentMap<String, CollectionShardState> myCollections;

    /**
     * The primary shard for the database. This is where we can find non-sharded
     * collections.
     */
    private volatile Shard myPrimaryShard;

    /** If the system thinks that the database supports sharding. */
    private volatile boolean mySharded;

    /**
     * Creates a new DatabaseShardState.
     * 
     * @param sharded
     *            If true the database supports sharding.
     * @param primaryShard
     *            The primary shard for the database. This is where we can find
     *            non-sharded collections.
     */
    public DatabaseShardState(final boolean sharded, final Shard primaryShard) {
        mySharded = sharded;
        myPrimaryShard = primaryShard;
        myCollections = new ConcurrentHashMap<String, CollectionShardState>();
    }

    /**
     * Deletes the state of for the specified database/collection. This should
     * be called when a
     * {@link com.allanbank.mongodb.error.ShardConfigStaleException} is
     * encountered.
     * 
     * @param collection
     *            The collection that the query or document is going to be used
     *            with.
     */
    public void deleteState(final String collection) {
        myCollections.remove(collection);
    }

    /**
     * Tries to determine the correct shard to receive the query/inserted
     * document.
     * 
     * @param collection
     *            The collection that the query or document is going to be used
     *            with.
     * @param queryOrDocument
     *            The query that will be used or the document that
     * @return The shard to use with the query/insert or <code>null</code> if a
     *         specific shard could not be determined.
     */
    public Shard findShardFor(final String collection,
            final Document queryOrDocument) {

        Shard result = myPrimaryShard;

        // If sharded/partitioned ...
        if (mySharded) {
            // ... use the CollectionShardState for the collection...
            final CollectionShardState collectionState = myCollections
                    .get(collection);
            if (collectionState != null) {
                result = collectionState.findShard(queryOrDocument);
            }
            else {
                // Return null since we don't know about the collection.
                result = null;

                // TODO: Request the state for the collection.
            }
        }

        return result;
    }

    /**
     * Returns the state of the collections within the database.
     * 
     * @return The state of the collections within the database.
     */
    public Map<String, CollectionShardState> getCollections() {
        return Collections.unmodifiableMap(myCollections);
    }

    /**
     * Returns the primary shard for the database. This is where we can find
     * non-sharded collections.
     * 
     * @return The primary shard for the database. This is where we can find
     *         non-sharded collections.
     */
    public Shard getPrimaryShard() {
        return myPrimaryShard;
    }

    /**
     * Returns true the system thinks that the database supports sharding.
     * 
     * @return True the system thinks that the database supports sharding.
     */
    public boolean isSharded() {
        return mySharded;
    }

    /**
     * Sets the state of the collections within the database.
     * 
     * @param collections
     *            The state of the collections.
     */
    public void setCollections(
            final Map<String, CollectionShardState> collections) {
        myCollections.clear();
        if (collections != null) {
            myCollections.putAll(collections);
        }
    }

    /**
     * Sets the value of primary shard to the new value. This is where we can
     * find non-sharded collections.
     * 
     * @param primaryShard
     *            The new value for the primary shard.
     */
    public void setPrimaryShard(final Shard primaryShard) {
        myPrimaryShard = primaryShard;
    }

    /**
     * Sets to true if true the system thinks that the database supports
     * sharding.
     * 
     * @param sharded
     *            The new value for if the system thinks that the database
     *            supports sharding.
     */
    public void setSharded(final boolean sharded) {
        mySharded = sharded;
    }
}
