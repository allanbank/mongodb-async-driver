/*
 * #%L
 * ListCollectionsMessage.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.ListIndexes;
import com.allanbank.mongodb.client.Message;

/**
 * Specialized message to handle transforming the listIndexes command into a
 * query on the {@code system.indexes} collection when sent to a server before
 * MongoDB 2.7.7.
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/command/listIndexes/">listIndexes
 *      Command</a>
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ListIndexesCommand
        extends Command
        implements CursorableMessage {

    /**
     * The first version of MongoDB to support the {@code listIndexes} command.
     */
    public static final Version COMMAND_VERSION = ListCollectionsCommand.COMMAND_VERSION;

    /**
     * Creates a new command document based on the collection name provided.
     *
     * @param collectionName
     *            The name of the collection to return the indexes for.
     * @param request
     *            The original request.
     * @return The {@code listIndexes} command document.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/listCollections/">listIndexes
     *      Command</a>
     */
    private static Document createCommand(final String collectionName,
            final ListIndexes request) {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.add("listIndexes", collectionName);
        if (request.getMaximumTimeMilliseconds() > 0) {
            builder.add("maxTimeMS", request.getMaximumTimeMilliseconds());
        }

        return builder.build();
    }

    /** The original request. */
    private final ListIndexes myRequest;

    /** If true then the cluster the command will be sent to is sharded. */
    private final boolean myShardedCluster;

    /**
     * Creates a new ListIndexesCommand.
     *
     * @param databaseName
     *            The name of the database we are reading the indexes for.
     * @param collectionName
     *            The name of the collection we are reading the indexes for.
     * @param request
     *            The original request.
     * @param readPreference
     *            The {@link ReadPreference} for which server's to use.
     * @param shardedCluster
     *            If true then the cluster the command will be sent to is
     *            sharded.
     */
    public ListIndexesCommand(final String databaseName,
            final String collectionName, final ListIndexes request,
            final ReadPreference readPreference, final boolean shardedCluster) {
        super(databaseName, collectionName, createCommand(collectionName,
                request), readPreference);

        myRequest = request;
        myShardedCluster = shardedCluster;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the batch size for the command.
     * </p>
     *
     * @see CursorableMessage#getBatchSize()
     */
    @Override
    public int getBatchSize() {
        return myRequest.getBatchSize();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the limit on the number of collection to return.
     * </p>
     *
     * @see CursorableMessage#getLimit()
     */
    @Override
    public int getLimit() {
        return myRequest.getLimit();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to transform the command into a {@link Query} on the
     * {@code system.indexes} collection when the server version is before the
     * {@link #COMMAND_VERSION command version}.
     * </p>
     *
     * @see Message#transformFor(Version)
     */
    @Override
    public Message transformFor(final Version serverVersion) {
        Message result = this;

        if (serverVersion.compareTo(COMMAND_VERSION) < 0) {
            // The server will not understand the 'listIndexes' command.
            // Fallback to a query on the 'system.indexes' collection.
            final DocumentBuilder query = BuilderFactory.start();

            query.add("ns", myDatabaseName + "." + myCollectionName);
            if (myRequest.getMaximumTimeMilliseconds() > 0) {
                query.add("$maxTimeMS", myRequest.getMaximumTimeMilliseconds());
            }
            if (myShardedCluster) {
                updateReadPreference(query, getReadPreference());
            }

            result = new Query(myDatabaseName, "system.indexes", query.build(),
            /* fields= */null, getBatchSize(), getLimit(),
            /* numberToSkip= */0,
            /* tailable= */false, getReadPreference(),
            /* noCursorTimeout= */false, /* awaitData= */false,
            /* exhaust= */false, /* partial= */false);
        }

        return result;
    }
}
