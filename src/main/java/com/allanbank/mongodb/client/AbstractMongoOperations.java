/*
 * #%L
 * AbstractMongoOperations.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.allanbank.mongodb.AsyncMongoCollection;
import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.ParallelScan;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.callback.BatchedNativeWriteCallback;
import com.allanbank.mongodb.client.callback.BatchedWriteCallback;
import com.allanbank.mongodb.client.callback.CursorCallback;
import com.allanbank.mongodb.client.callback.CursorStreamingCallback;
import com.allanbank.mongodb.client.callback.LongToIntCallback;
import com.allanbank.mongodb.client.callback.MultipleCursorCallback;
import com.allanbank.mongodb.client.callback.ReplyArrayCallback;
import com.allanbank.mongodb.client.callback.ReplyDocumentCallback;
import com.allanbank.mongodb.client.callback.ReplyIntegerCallback;
import com.allanbank.mongodb.client.callback.ReplyLongCallback;
import com.allanbank.mongodb.client.callback.ReplyResultCallback;
import com.allanbank.mongodb.client.callback.SingleDocumentCallback;
import com.allanbank.mongodb.client.message.AggregateCommand;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.ParallelScanCommand;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Update;

/**
 * AbstractMongoOperations provides the core functionality for the operations on
 * a MongoDB collection.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMongoOperations {

    /**
     * The default for if a delete should only delete the first document it
     * matches.
     */
    public static final boolean DELETE_SINGLE_DELETE_DEFAULT = false;

    /** The default empty index options. */
    public static final Document EMPTY_INDEX_OPTIONS = EmptyDocument.INSTANCE;

    /** The name of the canonical id field for MongoDB. */
    public static final String ID_FIELD_NAME = "_id";

    /** The default for if an insert should continue on an error. */
    public static final boolean INSERT_CONTINUE_ON_ERROR_DEFAULT = false;

    /** The default for a UNIQUE index options. */
    public static final Document UNIQUE_INDEX_OPTIONS = new ImmutableDocument(
            BuilderFactory.start().add("unique", true));

    /** The default for doing a multiple-update on an update. */
    public static final boolean UPDATE_MULTIUPDATE_DEFAULT = false;

    /** The default for doing an upsert on an update. */
    public static final boolean UPDATE_UPSERT_DEFAULT = false;

    /** The client for interacting with MongoDB. */
    protected final Client myClient;

    /** The name of the database we interact with. */
    protected final MongoDatabase myDatabase;

    /** The name of the collection we interact with. */
    protected final String myName;

    /** The {@link Durability} for writes from this database instance. */
    private Durability myDurability;

    /** The {@link ReadPreference} for reads from this database instance. */
    private ReadPreference myReadPreference;

    /**
     * Create a new AbstractAsyncMongoCollection.
     * 
     * @param client
     *            The client for interacting with MongoDB.
     * @param database
     *            The database we interact with.
     * @param name
     *            The name of the collection we interact with.
     */
    public AbstractMongoOperations(final Client client,
            final MongoDatabase database, final String name) {
        super();

        myClient = client;
        myDatabase = database;
        myName = name;
        myDurability = null;
        myReadPreference = null;
    }

    /**
     * Constructs a {@code aggregate} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback for the aggregation results returned.
     * @param command
     *            The details of the aggregation request.
     * @throws MongoDbException
     *             On an error executing the aggregate command.
     * @see AsyncMongoCollection#aggregateAsync(Callback, Aggregate)
     */
    public void aggregateAsync(final Callback<MongoIterator<Document>> results,
            final Aggregate command) throws MongoDbException {

        final AggregateCommand commandMsg = toCommand(command, false);

        final CursorCallback callback = new CursorCallback(myClient,
                commandMsg, true, results);

        myClient.send(commandMsg, callback);
    }

    /**
     * Constructs a {@code count} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            The callback to notify of the results.
     * @param count
     *            The count command.
     * @throws MongoDbException
     *             On an error counting the documents.
     * @see AsyncMongoCollection#countAsync(Callback, Count)
     */
    public void countAsync(final Callback<Long> results, final Count count)
            throws MongoDbException {
        Version minVersion = null;
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("count", getName());
        builder.addDocument("query", count.getQuery());
        if (count.getMaximumTimeMilliseconds() > 0) {
            minVersion = Count.MAX_TIMEOUT_VERSION;
            builder.add("maxTimeMS", count.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference finalPreference = updateReadPreference(builder,
                count.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), count.getQuery(), finalPreference,
                VersionRange.minimum(minVersion));

        myClient.send(commandMsg, new ReplyLongCallback(results));
    }

    /**
     * Constructs a {@link Delete} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback that will be notified of the results of the query. If
     *            the durability of the operation is NONE then this will be -1.
     * @param query
     *            Query to locate the documents to be deleted.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     * @param durability
     *            The durability for the delete.
     * @throws MongoDbException
     *             On an error deleting the documents.
     * @see AsyncMongoCollection#deleteAsync(Callback, DocumentAssignable,
     *      boolean, Durability)
     */
    public void deleteAsync(final Callback<Long> results,
            final DocumentAssignable query, final boolean singleDelete,
            final Durability durability) throws MongoDbException {

        if ((durability != Durability.NONE) && useWriteCommand()
                && isWriteCommandsSupported(null)) {

            final BatchedWrite write = BatchedWrite.delete(query, singleDelete,
                    durability);

            writeAsync(results, write);
        }
        else {
            final Delete deleteMessage = new Delete(getDatabaseName(), myName,
                    query.asDocument(), singleDelete);

            if (Durability.NONE.equals(durability)) {
                myClient.send(deleteMessage, null);
                results.callback(Long.valueOf(-1));
            }
            else {
                myClient.send(deleteMessage, asGetLastError(durability),
                        new ReplyLongCallback(results));
            }
        }
    }

    /**
     * Constructs a {@code distinct} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback for the distinct results returned.
     * @param command
     *            The details of the distinct request.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#distinctAsync(Callback, Distinct)
     */
    public void distinctAsync(final Callback<MongoIterator<Element>> results,
            final Distinct command) throws MongoDbException {

        Version minVersion = null;

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("distinct", getName());
        builder.addString("key", command.getKey());
        if (command.getQuery() != null) {
            builder.addDocument("query", command.getQuery());
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = Distinct.MAX_TIMEOUT_VERSION;
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), readPreference,
                VersionRange.minimum(minVersion));

        myClient.send(commandMsg, new ReplyArrayCallback(results));

    }

    /**
     * Constructs a {@link AggregateCommand} and sends it to the server via the
     * {@link Client}.
     * 
     * @param aggregation
     *            The aggregation details.
     * @param results
     *            Callback that will be notified of the results of the explain.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @since MongoDB 2.6
     * @see AsyncMongoCollection#explainAsync(Callback, Aggregate)
     */
    public void explainAsync(final Callback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        final AggregateCommand commandMsg = toCommand(aggregation, true);

        myClient.send(commandMsg, new SingleDocumentCallback(results));
    }

    /**
     * Constructs a {@link Query} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param query
     *            The query details.
     * @param results
     *            Callback that will be notified of the results of the explain.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#explainAsync(Callback, Find)
     */
    public void explainAsync(final Callback<Document> results, final Find query)
            throws MongoDbException {

        ReadPreference readPreference = query.getReadPreference();
        if (readPreference == null) {
            readPreference = getReadPreference();
        }

        Document queryDoc;
        if (!readPreference.isLegacy()
                && (myClient.getClusterType() == ClusterType.SHARDED)) {
            queryDoc = query.toQueryRequest(true, readPreference);
        }
        else {
            queryDoc = query.toQueryRequest(true);
        }

        final Query queryMessage = new Query(getDatabaseName(), myName,
                queryDoc, query.getProjection(), query.getBatchSize(),
                query.getLimit(), query.getNumberToSkip(),
                false /* tailable */, readPreference,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, query.isPartialOk());

        myClient.send(queryMessage, new SingleDocumentCallback(results));
    }

    /**
     * Constructs a {@code findAndModify} command and sends it to the server via
     * the {@link Client}.
     * 
     * @param results
     *            Callback for the the found document.
     * @param command
     *            The details of the find and modify request.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#findAndModifyAsync(Callback, FindAndModify)
     */
    public void findAndModifyAsync(final Callback<Document> results,
            final FindAndModify command) throws MongoDbException {
        Version minVersion = null;

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("findAndModify", getName());
        builder.addDocument("query", command.getQuery());
        if (command.getUpdate() != null) {
            builder.addDocument("update", command.getUpdate());
        }
        if (command.getSort() != null) {
            builder.addDocument("sort", command.getSort());
        }
        if (command.getFields() != null) {
            builder.addDocument("fields", command.getFields());
        }
        if (command.isRemove()) {
            builder.addBoolean("remove", true);
        }
        if (command.isReturnNew()) {
            builder.addBoolean("new", true);
        }
        if (command.isUpsert()) {
            builder.addBoolean("upsert", true);
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = FindAndModify.MAX_TIMEOUT_VERSION;
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Must be the primary since this is a write.
        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), command.getQuery(), ReadPreference.PRIMARY,
                VersionRange.minimum(minVersion));
        myClient.send(commandMsg, new ReplyDocumentCallback(results));
    }

    /**
     * Constructs a {@link Query} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param query
     *            The query details.
     * @param results
     *            Callback that will be notified of the results of the find.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#findAsync(Callback, Find)
     */
    public void findAsync(final Callback<MongoIterator<Document>> results,
            final Find query) throws MongoDbException {

        final Query queryMessage = createQuery(query, query.getLimit(),
                query.getBatchSize(), query.isTailable(), query.isAwaitData(),
                query.isImmortalCursor());

        final CursorCallback callback = new CursorCallback(myClient,
                queryMessage, false, results);

        myClient.send(queryMessage, callback);
    }

    /**
     * Constructs a {@link Query} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param query
     *            The query details.
     * @param results
     *            Callback that will be notified of the results of the find.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#findOneAsync(Callback, Find)
     */
    public void findOneAsync(final Callback<Document> results, final Find query)
            throws MongoDbException {
        final Query queryMessage = createQuery(query, 1, 1, false, false, false);

        myClient.send(queryMessage, new SingleDocumentCallback(results));
    }

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getDatabaseName() {
        return myDatabase.getName();
    }

    /**
     * Returns the durability to use when no durability is specified for the
     * write operation.
     * 
     * @return The durability to use when no durability is specified for the
     *         write operation.
     */
    public Durability getDurability() {
        Durability result = myDurability;
        if (result == null) {
            result = myDatabase.getDurability();
        }
        return result;
    }

    /**
     * Returns the name of the collection.
     * 
     * @return The name of the collection.
     */
    public String getName() {
        return myName;
    }

    /**
     * Returns the read preference to use when no read preference is specified
     * for the read operation.
     * 
     * @return The read preference to use when no read preference is specified
     *         for the read operation.
     */
    public ReadPreference getReadPreference() {
        ReadPreference result = myReadPreference;
        if (result == null) {
            result = myDatabase.getReadPreference();
        }
        return result;
    }

    /**
     * Constructs a {@code group} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback for the group results returned.
     * @param command
     *            The details of the group request.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#groupByAsync(Callback, GroupBy)
     */
    public void groupByAsync(final Callback<MongoIterator<Element>> results,
            final GroupBy command) throws MongoDbException {
        Version minVersion = null;

        final DocumentBuilder builder = BuilderFactory.start();

        final DocumentBuilder groupDocBuilder = builder.push("group");

        groupDocBuilder.addString("ns", getName());
        if (!command.getKeys().isEmpty()) {
            final DocumentBuilder keysBuilder = groupDocBuilder.push("key");
            for (final String key : command.getKeys()) {
                keysBuilder.addBoolean(key, true);
            }
        }
        if (command.getKeyFunction() != null) {
            groupDocBuilder.addJavaScript("$keyf", command.getKeyFunction());
        }
        if (command.getInitialValue() != null) {
            groupDocBuilder.addDocument("initial", command.getInitialValue());
        }
        if (command.getReduceFunction() != null) {
            groupDocBuilder.addJavaScript("$reduce",
                    command.getReduceFunction());
        }
        if (command.getFinalizeFunction() != null) {
            groupDocBuilder.addJavaScript("finalize",
                    command.getFinalizeFunction());
        }
        if (command.getQuery() != null) {
            groupDocBuilder.addDocument("cond", command.getQuery());
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = GroupBy.MAX_TIMEOUT_VERSION;
            // maxTimeMS is not in the "group" sub-doc.
            // See SERVER-12595 commands.
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(
                groupDocBuilder, command.getReadPreference(), false);

        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), readPreference,
                VersionRange.minimum(minVersion));
        myClient.send(commandMsg, new ReplyArrayCallback("retval", results));
    }

    /**
     * Constructs a {@link Insert} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. Currently, the value is always zero. Once <a
     *            href="http://jira.mongodb.org/browse/SERVER-4381"
     *            >SERVER-4381</a> is fixed then expected to be the number of
     *            documents inserted. If the durability is NONE then returns
     *            <code>-1</code>.
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     * @see AsyncMongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable...)
     */
    public void insertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final DocumentAssignable... documents) throws MongoDbException {

        doInsertAsync(results, continueOnError, durability, null, documents);
    }

    /**
     * Constructs a {@code mapreduce} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback for the map/reduce results returned. Note this might
     *            be empty if the output type is not inline.
     * @param command
     *            The details of the map/reduce request.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#mapReduceAsync(Callback, MapReduce)
     */
    public void mapReduceAsync(final Callback<MongoIterator<Document>> results,
            final MapReduce command) throws MongoDbException {
        Version minVersion = null;

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("mapreduce", getName());
        builder.addJavaScript("map", command.getMapFunction());
        builder.addJavaScript("reduce", command.getReduceFunction());
        if (command.getFinalizeFunction() != null) {
            builder.addJavaScript("finalize", command.getFinalizeFunction());
        }
        if (command.getQuery() != null) {
            builder.addDocument("query", command.getQuery());
        }
        if (command.getSort() != null) {
            builder.addDocument("sort", command.getSort());
        }
        if (command.getScope() != null) {
            builder.addDocument("scope", command.getScope());
        }
        if (command.getLimit() != 0) {
            builder.addInteger("limit", command.getLimit());
        }
        if (command.isKeepTemp()) {
            builder.addBoolean("keeptemp", true);
        }
        if (command.isJsMode()) {
            builder.addBoolean("jsMode", true);
        }
        if (command.isVerbose()) {
            builder.addBoolean("verbose", true);
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = MapReduce.MAX_TIMEOUT_VERSION;
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        final DocumentBuilder outputBuilder = builder.push("out");
        switch (command.getOutputType()) {
        case INLINE: {
            outputBuilder.addInteger("inline", 1);
            break;
        }
        case REPLACE: {
            outputBuilder.addString("replace", command.getOutputName());
            if (command.getOutputDatabase() != null) {
                outputBuilder.addString("db", command.getOutputDatabase());
            }
            break;
        }
        case MERGE: {
            outputBuilder.addString("merge", command.getOutputName());
            if (command.getOutputDatabase() != null) {
                outputBuilder.addString("db", command.getOutputDatabase());
            }
            break;
        }
        case REDUCE: {
            outputBuilder.addString("reduce", command.getOutputName());
            if (command.getOutputDatabase() != null) {
                outputBuilder.addString("db", command.getOutputDatabase());
            }
            break;
        }
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), readPreference,
                VersionRange.minimum(minVersion));
        myClient.send(commandMsg, new ReplyResultCallback(results));
    }

    /**
     * Constructs a {@code parallelCollectionScan} command and sends it to the
     * server via the {@link Client}.
     * 
     * @param results
     *            Callback for the collection of iterators.
     * @param parallelScan
     *            The details on the scan.
     * @throws MongoDbException
     *             On an error initializing the parallel scan.
     * @see AsyncMongoCollection#parallelScanAsync(Callback, ParallelScan)
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/parallelCollectionScan/">parallelCollectionScan
     *      Command</a>
     */
    public void parallelScanAsync(
            final Callback<Collection<MongoIterator<Document>>> results,
            final ParallelScan parallelScan) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.add("parallelCollectionScan", getName());
        builder.add("numCursors", parallelScan.getRequestedIteratorCount());

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                parallelScan.getReadPreference(), true);

        final ParallelScanCommand commandMsg = new ParallelScanCommand(
                parallelScan, getDatabaseName(), getName(), builder.build(),
                readPreference);

        myClient.send(commandMsg, new MultipleCursorCallback(myClient,
                commandMsg, results));

    }

    /**
     * Constructs a {@link Insert} of {@link Update} message based on if the
     * document contains a {@link #ID_FIELD_NAME} and sends it to the server via
     * the {@link Client}.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. If the durability of the operation is NONE then this
     *            will be -1.
     * @param document
     *            The document to save to the collection.
     * @param durability
     *            The durability for the save.
     * @throws MongoDbException
     *             On an error saving the documents.
     * @see AsyncMongoCollection#saveAsync(Callback, DocumentAssignable,
     *      Durability)
     */
    public void saveAsync(final Callback<Integer> results,
            final DocumentAssignable document, final Durability durability)
            throws MongoDbException {
        final Document doc = document.asDocument();

        if (doc.contains(ID_FIELD_NAME)) {
            updateAsync(new LongToIntCallback(results), BuilderFactory.start()
                    .add(doc.get(ID_FIELD_NAME)), doc, false, true, durability);
        }
        else {
            insertAsync(results, INSERT_CONTINUE_ON_ERROR_DEFAULT, durability,
                    doc);
        }
    }

    /**
     * Sets the durability to use when no durability is specified for the write
     * operation.
     * 
     * @param durability
     *            The durability to use when no durability is specified for the
     *            write operation.
     */
    public void setDurability(final Durability durability) {
        myDurability = durability;
    }

    /**
     * Sets the read preference to use when no read preference is specified for
     * the read operation.
     * 
     * @param readPreference
     *            The read preference to use when no read preference is
     *            specified for the read operation.
     */
    public void setReadPreference(final ReadPreference readPreference) {
        myReadPreference = readPreference;
    }

    /**
     * Constructs a {@code aggregate} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback that will be notified of the results of the query.
     * @param aggregation
     *            The aggregation details.
     * @return A {@link MongoCursorControl} to control the cursor streaming
     *         documents to the caller. This includes the ability to stop the
     *         cursor and persist its state.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#stream(StreamCallback, Aggregate)
     */
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        final AggregateCommand commandMsg = toCommand(aggregation, false);

        final CursorStreamingCallback callback = new CursorStreamingCallback(
                myClient, commandMsg, true, results);

        myClient.send(commandMsg, callback);

        return callback;
    }

    /**
     * Constructs a {@link Query} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback that will be notified of the results of the query.
     * @param query
     *            The query details.
     * @return A {@link MongoCursorControl} to control the cursor streaming
     *         documents to the caller. This includes the ability to stop the
     *         cursor and persist its state.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @see AsyncMongoCollection#stream(StreamCallback, Find)
     */
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Find query) throws MongoDbException {
        final Query queryMessage = createQuery(query, query.getLimit(),
                query.getBatchSize(), query.isTailable(), query.isAwaitData(),
                query.isImmortalCursor());

        final CursorStreamingCallback callback = new CursorStreamingCallback(
                myClient, queryMessage, false, results);

        myClient.send(queryMessage, callback);

        return callback;
    }

    /**
     * Constructs a {@code text} command and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            Callback for the {@code text} results returned.
     * @param command
     *            The details of the {@code text} request.
     * @throws MongoDbException
     *             On an error executing the {@code text} command.
     * @see <a
     *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-queries">
     *      MongoDB Text Queries</a>
     * @since MongoDB 2.4
     * @see AsyncMongoCollection#textSearchAsync(Callback,
     *      com.allanbank.mongodb.builder.Text)
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This method will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    public void textSearchAsync(
            final Callback<MongoIterator<com.allanbank.mongodb.builder.TextResult>> results,
            final com.allanbank.mongodb.builder.Text command)
            throws MongoDbException {
        final Version minVersion = com.allanbank.mongodb.builder.Text.REQUIRED_VERSION;
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("text", getName());
        builder.addString("search", command.getSearchTerm());
        if (command.getQuery() != null) {
            builder.add("filter", command.getQuery());
        }
        if (command.getLimit() > 0) {
            builder.add("limit", command.getLimit());
        }
        if (command.getReturnFields() != null) {
            builder.add("project", command.getReturnFields());
        }
        if (command.getLanguage() != null) {
            builder.add("language", command.getLanguage());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(), getName(),
                builder.build(), readPreference,
                VersionRange.minimum(minVersion));
        myClient.send(commandMsg,
                new ReplyResultCallback(
                        new com.allanbank.mongodb.client.callback.TextCallback(
                                results)));
    }

    /**
     * Constructs a {@link Update} message and sends it to the server via the
     * {@link Client}.
     * 
     * @param results
     *            The {@link Callback} that will be notified of the number of
     *            documents updated. If the durability of the operation is NONE
     *            then this will be -1.
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @param multiUpdate
     *            If true then the update is applied to all of the matching
     *            documents, otherwise only the first document found is updated.
     * @param upsert
     *            If true then if no document is found then a new document is
     *            created and updated, otherwise no operation is performed.
     * @param durability
     *            The durability for the update.
     * @throws MongoDbException
     *             On an error updating the documents.
     * @see AsyncMongoCollection#updateAsync(Callback, DocumentAssignable,
     *      DocumentAssignable, boolean, boolean, Durability)
     */
    public void updateAsync(final Callback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException {

        final ClusterStats stats = myClient.getClusterStats();
        if ((durability != Durability.NONE) && useWriteCommand()
                && isWriteCommandsSupported(stats)) {
            final BatchedWrite write = BatchedWrite.update(query, update,
                    multiUpdate, upsert, durability);

            doWriteAsync(stats, results, write);
        }
        else {
            final Update updateMessage = new Update(getDatabaseName(), myName,
                    query.asDocument(), update.asDocument(), multiUpdate,
                    upsert);

            if (Durability.NONE == durability) {
                myClient.send(updateMessage, null);
                results.callback(Long.valueOf(-1));
            }
            else {
                myClient.send(updateMessage, asGetLastError(durability),
                        new ReplyLongCallback(results));
            }
        }
    }

    /**
     * Constructs the appropriate set of write commands to send to the server.
     * 
     * @param results
     *            The {@link Callback} that will be notified of the number of
     *            documents inserted, updated, and deleted. If the durability of
     *            the operation is NONE then this will be -1.
     * @param write
     *            The batched writes
     * @throws MongoDbException
     *             On an error submitting the write operations.
     * 
     * @since MongoDB 2.6
     * @see AsyncMongoCollection#writeAsync(Callback,BatchedWrite)
     */
    public void writeAsync(final Callback<Long> results,
            final BatchedWrite write) throws MongoDbException {
        final ClusterStats stats = myClient.getClusterStats();

        doWriteAsync(stats, results, write);
    }

    /**
     * Converts the {@link Durability} into a {@link GetLastError} command.
     * 
     * @param durability
     *            The {@link Durability} to convert.
     * @return The {@link GetLastError} command.
     */
    protected GetLastError asGetLastError(final Durability durability) {
        return new GetLastError(getDatabaseName(), durability);
    }

    /**
     * Creates a properly configured {@link Query} message.
     * 
     * @param query
     *            The {@link Find} to construct the {@link Query} from.
     * @param limit
     *            The limit for the query.
     * @param batchSize
     *            The batch size for the query.
     * @param tailable
     *            If the query should create a tailable cursor.
     * @param awaitData
     *            If the query should await data.
     * @param immortal
     *            If the query should create a cursor that does not timeout,
     *            e.g., immortal.
     * @return The {@link Query} message.
     */
    protected Query createQuery(final Find query, final int limit,
            final int batchSize, final boolean tailable,
            final boolean awaitData, final boolean immortal) {
        ReadPreference readPreference = query.getReadPreference();
        if (readPreference == null) {
            readPreference = getReadPreference();
        }

        Document queryDoc;
        if (!readPreference.isLegacy()
                && (myClient.getClusterType() == ClusterType.SHARDED)) {
            queryDoc = query.toQueryRequest(false, readPreference);
        }
        else {
            queryDoc = query.toQueryRequest(false);
        }

        return new Query(getDatabaseName(), myName, queryDoc,
                query.getProjection(), batchSize, limit,
                query.getNumberToSkip(), tailable, readPreference, immortal,
                awaitData, false /* exhaust */, query.isPartialOk());
    }

    /**
     * Sends an {@link Insert} message to the server. This version is private to
     * this class since most inserts do not need the server version.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert.
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param durability
     *            The durability for the insert.
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    protected void doInsertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final Version requiredServerVersion,
            final DocumentAssignable... documents) throws MongoDbException {

        final ClusterStats stats = myClient.getClusterStats();
        if ((durability != Durability.NONE) && useWriteCommand()
                && isWriteCommandsSupported(stats)) {
            final BatchedWrite write = BatchedWrite.insert(continueOnError,
                    durability, documents);

            doWriteAsync(stats, new LongToIntCallback(results), write);
        }
        else {
            // Make sure the documents have an _id.
            final List<Document> docs = new ArrayList<Document>(
                    documents.length);
            for (final DocumentAssignable docAssignable : documents) {
                final Document doc = docAssignable.asDocument();
                if (!doc.contains(ID_FIELD_NAME)
                        && (doc instanceof RootDocument)) {
                    ((RootDocument) doc).injectId();
                }
                docs.add(doc);
            }

            final Insert insertMessage = new Insert(getDatabaseName(), myName,
                    docs, continueOnError,
                    VersionRange.minimum(requiredServerVersion));
            if (Durability.NONE == durability) {
                myClient.send(insertMessage, null);
                results.callback(Integer.valueOf(-1));
            }
            else {
                myClient.send(insertMessage, asGetLastError(durability),
                        new ReplyIntegerCallback(results));
            }
        }
    }

    /**
     * Performs a async write operation.
     * 
     * @param stats
     *            The stats for verifying the server support write commands.
     * @param results
     *            The callback for the write.
     * @param write
     *            The write to send.
     */
    protected void doWriteAsync(final ClusterStats stats,
            final Callback<Long> results, final BatchedWrite write) {
        if (isWriteCommandsSupported(stats)) {

            final List<BatchedWrite.Bundle> bundles = write.toBundles(
                    getName(), stats.getSmallestMaxBsonObjectSize(),
                    stats.getSmallestMaxBatchedWriteOperations());
            if (bundles.isEmpty()) {
                results.callback(Long.valueOf(0));
                return;
            }

            final BatchedWriteCallback callback = new BatchedWriteCallback(
                    getDatabaseName(), getName(), results, write, myClient,
                    bundles);

            // Push the messages out.
            callback.send();
        }
        else {
            final List<WriteOperation> operations = write.getWrites();
            if (operations.isEmpty()) {
                results.callback(Long.valueOf(0));
                return;
            }

            final BatchedNativeWriteCallback callback = new BatchedNativeWriteCallback(
                    results, write, this, operations);

            // Push the messages out.
            callback.send();
        }
    }

    /**
     * Determines if all of the servers in the cluster support the write
     * commands.
     * 
     * @param stats
     *            The cluster stats if they have already been retrieved.
     * @return True if all servers in the cluster are at least the
     *         {@link BatchedWrite#REQUIRED_VERSION}.
     */
    protected boolean isWriteCommandsSupported(final ClusterStats stats) {
        final ClusterStats clusterStats = (stats == null) ? myClient
                .getClusterStats() : stats;
        final VersionRange serverVersionRange = clusterStats
                .getServerVersionRange();
        final Version minServerVersion = serverVersionRange.getLowerBounds();

        return (BatchedWrite.REQUIRED_VERSION.compareTo(minServerVersion) <= 0);
    }

    /**
     * Converts the {@link Aggregate} object to an {@link AggregateCommand}.
     * 
     * @param command
     *            The {@link Aggregate} to convert.
     * @param explain
     *            If rue then have the server explain the aggregation instead of
     *            performing the aggregation.
     * @return The command to send to the server for the {@link Aggregate}.
     */
    protected AggregateCommand toCommand(final Aggregate command,
            final boolean explain) {
        Version minVersion = command.getRequiredVersion();

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("aggregate", getName());

        // Pipeline of operations.
        final ArrayBuilder pipeline = builder.pushArray("pipeline");
        for (final Element e : command.getPipeline()) {
            pipeline.add(e);
        }

        // Options
        if (command.isAllowDiskUsage()) {
            builder.add("allowDiskUsage", true);
        }
        if (command.isUseCursor()) {
            final DocumentBuilder cursor = builder.push("cursor");
            if (command.getBatchSize() > 0) {
                cursor.add("batchSize", command.getBatchSize());
            }
        }
        if (explain) {
            minVersion = Version.later(minVersion, Aggregate.EXPLAIN_VERSION);
            builder.add("explain", true);
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final AggregateCommand commandMsg = new AggregateCommand(command,
                getDatabaseName(), getName(), builder.build(), readPreference,
                VersionRange.minimum(minVersion));
        return commandMsg;
    }

    /**
     * Determines the {@link ReadPreference} to be used based on the command's
     * {@code ReadPreference} or the collection's if the command's
     * {@code ReadPreference} is <code>null</code>. Updates the command's
     * {@link DocumentBuilder} with the {@code ReadPreference} details if
     * connected to a sharded cluster and the resulting {@code ReadPreference}
     * is not supported by the legacy settings.
     * 
     * @param builder
     *            The builder for the command document to augment with the read
     *            preferences if connected to a sharded cluster.
     * @param commandReadPreference
     *            The read preferences from the command.
     * @param createQueryElement
     *            If true then the existing builder's contents will be pushed
     *            into a $query sub-document. This is required to ensure the
     *            command is not rejected by the {@code mongod} after processing
     *            by the {@code mongos}.
     * @return The {@link ReadPreference} to use.
     */
    protected ReadPreference updateReadPreference(
            final DocumentBuilder builder,
            final ReadPreference commandReadPreference,
            final boolean createQueryElement) {

        ReadPreference readPreference = commandReadPreference;
        if (readPreference == null) {
            readPreference = getReadPreference();
        }

        if (!readPreference.isLegacy()
                && (myClient.getClusterType() == ClusterType.SHARDED)) {
            if (createQueryElement) {
                final Document query = builder.asDocument();
                builder.reset();
                builder.add("$query", query);
            }
            builder.add(ReadPreference.FIELD_NAME, readPreference);
        }

        return readPreference;
    }

    /**
     * Extension point for derived classes to force the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * ,
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * , and
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * methods to use the legacy {@link Insert}, {@link Update}, and
     * {@link Delete} messages regardless of the server version.
     * <p>
     * This version of the method always returns true.
     * </p>
     * 
     * @return Return true to allow the use of the write commands added in
     *         MongoDB 2.6. Use false to force the use of the {@link Insert},
     *         {@link Update}, and {@link Delete}
     */
    protected boolean useWriteCommand() {
        return true;
    }
}
