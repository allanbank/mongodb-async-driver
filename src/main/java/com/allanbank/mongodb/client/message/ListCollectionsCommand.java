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

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.util.regex.Pattern;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.builder.ListCollections;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.error.ServerVersionException;

/**
 * Specialized message to handle transforming the listCollection command into a
 * query on the {@code system.namespaces} collection when sent to a server
 * before MongoDB 2.7.7.
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/command/listCollections/">listCollections
 *      Command</a>
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ListCollectionsCommand
        extends Command
        implements CursorableMessage {

    /**
     * The first version of MongoDB to support the {@code listCollections}
     * command.
     */
    public static final Version COMMAND_VERSION = Version.parse("2.7.7");

    /**
     * The pattern to remove index names (those with a '$' in the name) when
     * querying a server before {@link #COMMAND_VERSION}.
     */
    public static final Pattern NON_INDEX_REGEX = Pattern.compile("^[^$]*$");

    /** The empty filter document. */
    private static final Document EMPTY_FILTER = EmptyDocument.INSTANCE;

    /**
     * Creates a new command document based on the filter provided.
     *
     * @param request
     *            The details on the request.
     * @param readPreference
     *            The resolved {@link ReadPreference} for which server's to use.
     * @param shardedCluster
     *            If true then the cluster the command will be sent to is
     *            sharded.
     * @return The {@code listCollections} command document.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/listCollections/">listCollections
     *      Command</a>
     */
    private static Document createCommand(final ListCollections request,
            final ReadPreference readPreference, final boolean shardedCluster) {
        final DocumentBuilder builder = BuilderFactory.start();
        final Document filter = request.getQuery();

        builder.add("listCollections", 1);
        if ((filter != null) && !EMPTY_FILTER.equals(filter)) {
            builder.add("filter", filter);
        }
        if (request.getMaximumTimeMilliseconds() > 0) {
            builder.add("maxTimeMS", request.getMaximumTimeMilliseconds());
        }
        if (shardedCluster) {
            updateReadPreference(builder, readPreference);
        }

        return builder.build();
    }

    /** The details on the request. */
    private final ListCollections myRequest;

    /** If true then the cluster the command will be sent to is sharded. */
    private final boolean myShardedCluster;

    /**
     * Creates a new ListCollectionsMessage.
     *
     * @param databaseName
     *            The name of the database we are reading the collections for.
     * @param request
     *            The details on the request.
     * @param readPreference
     *            The resolved {@link ReadPreference} for which server's to use.
     * @param shardedCluster
     *            If true then the cluster the command will be sent to is
     *            sharded.
     */
    public ListCollectionsCommand(final String databaseName,
            final ListCollections request, final ReadPreference readPreference,
            final boolean shardedCluster) {
        super(databaseName, Command.COMMAND_COLLECTION, createCommand(request,
                readPreference, shardedCluster), readPreference);
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
     * {@code system.namepaces} collection when the server version is before the
     * {@link #COMMAND_VERSION command version}.
     * </p>
     *
     * @see Message#transformFor(Version)
     */
    @Override
    public Message transformFor(final Version serverVersion)
            throws ServerVersionException {
        Message result = this;

        // If the server will not understand the 'listCollections' command.
        // fall-back to a query on the 'system.namespaces' collection.
        if (serverVersion.compareTo(COMMAND_VERSION) < 0) {
            // Have to fix the name filter since pre-2.7.7 we need a
            // <db>.<collection> instead of just the <collection>. We also use a
            // regular expression to filter out the index names (those with a
            // '$' in the name).
            final Document origFilter = myRequest.getQuery();
            final DocumentBuilder adaptedFilter = BuilderFactory
                    .start((origFilter != null) ? origFilter : EMPTY_FILTER);
            if ((origFilter != null) && origFilter.contains("name")) {
                adaptedFilter.remove("name");

                final StringElement name = origFilter.findFirst(
                        StringElement.class, "name");
                if (name != null) {
                    adaptedFilter.add("name",
                            getDatabaseName() + "." + name.getValue());

                    final Document userQuery = adaptedFilter.build();

                    adaptedFilter.reset();
                    adaptedFilter.pushArray("$and").add(userQuery)
                            .add(where("name").matches(NON_INDEX_REGEX));
                }
                else {
                    throw new ServerVersionException(getOperationName(),
                            VersionRange.minimum(COMMAND_VERSION),
                            serverVersion, this);
                }
            }
            else {
                adaptedFilter.add("name", NON_INDEX_REGEX);
            }

            // Add the max time constraint.
            if (myRequest.getMaximumTimeMilliseconds() > 0) {
                adaptedFilter.add("$maxTimeMS",
                        myRequest.getMaximumTimeMilliseconds());
            }

            if (myShardedCluster) {
                updateReadPreference(adaptedFilter, getReadPreference());
            }

            result = new Query(myDatabaseName, "system.namespaces",
                    adaptedFilter.build(),
                    /* fields= */null, getBatchSize(), getLimit(),
                    /* numberToSkip= */0,
                    /* tailable= */false, getReadPreference(),
                    /* noCursorTimeout= */false, /* awaitData= */false,
                    /* exhaust= */false, /* partial= */false);
        }

        return result;
    }
}
