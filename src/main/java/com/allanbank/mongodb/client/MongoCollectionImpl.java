/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.Insert;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Update;

/**
 * Implementation of the {@link MongoCollection} interface.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoCollectionImpl extends AbstractMongoCollection {

    /** The name of the canonical id field for MongoDB. */
    public static final String ID_FIELD_NAME = "_id";

    /**
     * Create a new MongoDatabaseClient.
     * 
     * @param client
     *            The client for interacting with MongoDB.
     * @param database
     *            The database the collection is a part of.
     * @param name
     *            The name of the collection we interact with.
     */
    public MongoCollectionImpl(final Client client,
            final MongoDatabase database, final String name) {
        super(client, database, name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a aggregate command and send it to the server.
     * </p>
     * 
     * @see MongoCollection#aggregateAsync(Callback, Aggregate)
     */
    @Override
    public void aggregateAsync(final Callback<List<Document>> results,
            final Aggregate command) throws MongoDbException {

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("aggregate", getName());

        final ArrayBuilder pipeline = builder.pushArray("pipeline");
        for (final Element e : command.getPipeline()) {
            pipeline.add(e);
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference);
        myClient.send(commandMsg, new ReplyResultCallback("result", results));

    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>count</code> method that implementations must
     * override.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results,
            final DocumentAssignable query, final ReadPreference readPreference)
            throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("count", getName());
        builder.addDocument("query", query.asDocument());

        // Should be last since might wrap command in a $query element.
        final ReadPreference finalPreference = updateReadPreference(builder,
                readPreference, true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), finalPreference);

        myClient.send(commandMsg, new ReplyLongCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to insert the index document into the 'system.indexes'
     * collection.
     * </p>
     * 
     * @see MongoCollection#createIndex(String,DocumentAssignable,Element...)
     */
    @Override
    public void createIndex(final String name,
            final DocumentAssignable options, final Element... keys)
            throws MongoDbException {

        String indexName = name;
        if ((name == null) || name.isEmpty()) {
            indexName = buildIndexName(keys);
        }

        final DocumentBuilder indexEntryBuilder = BuilderFactory.start();
        indexEntryBuilder.addString("name", indexName);
        indexEntryBuilder.addString("ns", getDatabaseName() + "." + getName());

        final DocumentBuilder keyBuilder = indexEntryBuilder.push("key");
        for (final Element key : keys) {
            keyBuilder.add(key);
        }

        for (final Element option : options.asDocument()) {
            indexEntryBuilder.add(option);
        }

        final MongoCollection indexCollection = new MongoCollectionImpl(
                myClient, myDatabase, "system.indexes");
        final Document indexDocument = indexEntryBuilder.build();
        if (indexCollection.findOne(indexDocument) == null) {
            indexCollection.insert(Durability.ACK, indexDocument);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Delete} message to the server.
     * </p>
     */
    @Override
    public void deleteAsync(final Callback<Long> results,
            final DocumentAssignable query, final boolean singleDelete,
            final Durability durability) throws MongoDbException {
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a 'distinct' command and send it to the server.
     * </p>
     */
    @Override
    public void distinctAsync(final Callback<ArrayElement> results,
            final Distinct command) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("distinct", getName());
        builder.addString("key", command.getKey());
        if (command.getQuery() != null) {
            builder.addDocument("query", command.getQuery());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference);

        myClient.send(commandMsg, new ReplyArrayCallback(results));

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a { "drop" : <collection_name> } command.
     * </p>
     * 
     * @see MongoCollection#drop()
     */
    @Override
    public boolean drop() {
        final Document result = myDatabase.runCommand("drop", myName, null);
        final List<NumericElement> okElem = result.find(NumericElement.class,
                "ok");

        return ((okElem.size() > 0) && (okElem.get(0).getIntValue() > 0));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a { "deleteIndexes" : <collection_name>, index :
     * <name> } command.
     * </p>
     */
    @Override
    public boolean dropIndex(final String name) throws MongoDbException {

        final DocumentBuilder options = BuilderFactory.start();
        options.addString("index", name);

        final Document result = myDatabase.runCommand("deleteIndexes", myName,
                options.build());
        final List<NumericElement> okElem = result.find(NumericElement.class,
                "ok");

        return ((okElem.size() > 0) && (okElem.get(0).getIntValue() > 0));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server to explain the
     * {@link Find}'s query.
     * </p>
     */
    @Override
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
                queryDoc, query.getReturnFields(), query.getBatchSize(),
                query.getLimit(), query.getNumberToSkip(),
                false /* tailable */, readPreference,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, query.isPartialOk());

        myClient.send(queryMessage, new QueryOneCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send an {@link Command} findAndModify message to the
     * server.
     * </p>
     * 
     * @see MongoCollection#findAndModifyAsync(Callback, FindAndModify)
     */
    @Override
    public void findAndModifyAsync(final Callback<Document> results,
            final FindAndModify command) throws MongoDbException {
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

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build());
        myClient.send(commandMsg, new ReplyDocumentCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server.
     * </p>
     */
    @Override
    public void findAsync(final Callback<ClosableIterator<Document>> results,
            final Find query) throws MongoDbException {

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

        final Query queryMessage = new Query(getDatabaseName(), myName,
                queryDoc, query.getReturnFields(), query.getBatchSize(),
                query.getLimit(), query.getNumberToSkip(),
                query.isTailable() /* tailable */, readPreference,
                false /* noCursorTimeout */,
                query.isTailable() /* awaitData */, false /* exhaust */,
                query.isPartialOk());

        final QueryCallback callback = new QueryCallback(myClient,
                queryMessage, results);
        final String address = myClient.send(queryMessage, callback);

        callback.setAddress(address);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server.
     * </p>
     * 
     * @see MongoCollection#findOneAsync(Callback, DocumentAssignable)
     */
    @Override
    public void findOneAsync(final Callback<Document> results,
            final DocumentAssignable query) throws MongoDbException {

        final ReadPreference readPreference = getReadPreference();

        Document queryDoc = query.asDocument();
        if (!readPreference.isLegacy()
                && (myClient.getClusterType() == ClusterType.SHARDED)) {
            final DocumentBuilder builder = BuilderFactory.start();

            builder.add("query", queryDoc);
            builder.addDocument(ReadPreference.FIELD_NAME,
                    readPreference.asDocument());

            queryDoc = builder.build();
        }

        final Query queryMessage = new Query(getDatabaseName(), myName,
                queryDoc, null, 1 /* batchSize */, 1 /* limit */, 0 /* skip */,
                false /* tailable */, readPreference,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        myClient.send(queryMessage, new QueryOneCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a group command and send it to the server.
     * </p>
     */
    @Override
    public void groupByAsync(final Callback<ArrayElement> results,
            final GroupBy command) throws MongoDbException {
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

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(
                groupDocBuilder, command.getReadPreference(), false);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference);
        myClient.send(commandMsg, new ReplyArrayCallback("retval", results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send an {@link Insert} message to the server.
     * </p>
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final DocumentAssignable... documents) throws MongoDbException {

        // Make sure the documents have an _id.
        final List<Document> docs = new ArrayList<Document>(documents.length);
        for (final DocumentAssignable docAssignable : documents) {
            final Document doc = docAssignable.asDocument();
            if (!doc.contains(ID_FIELD_NAME) && (doc instanceof RootDocument)) {
                ((RootDocument) doc).injectId();
            }
            docs.add(doc);
        }

        final Insert insertMessage = new Insert(getDatabaseName(), myName,
                docs, continueOnError);
        if (Durability.NONE == durability) {
            myClient.send(insertMessage, null);
            results.callback(Integer.valueOf(-1));
        }
        else {
            myClient.send(insertMessage, asGetLastError(durability),
                    new ReplyIntegerCallback(results));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@code collStats} command to the MongoDB server and
     * look for the {@code capped} field to determine if the collection is
     * capped or not.
     * </p>
     * 
     * @see MongoCollection#isCapped
     */
    @Override
    public boolean isCapped() throws MongoDbException {
        final Document statistics = stats();

        final NumericElement numeric = statistics.get(NumericElement.class,
                "capped");
        if (numeric != null) {
            return (numeric.getIntValue() != 0);
        }

        final BooleanElement bool = statistics.get(BooleanElement.class,
                "capped");
        if (bool != null) {
            return bool.getValue();
        }

        // Not found implies not capped.
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a mapReduce command and send it to the server.
     * </p>
     * 
     * @see MongoCollection#mapReduceAsync(Callback, MapReduce)
     */
    @Override
    public void mapReduceAsync(final Callback<List<Document>> results,
            final MapReduce command) throws MongoDbException {
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

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference);
        myClient.send(commandMsg, new ReplyResultCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to save the document.
     * </p>
     */
    @Override
    public void saveAsync(final Callback<Integer> results,
            final DocumentAssignable document, final Durability durability)
            throws MongoDbException {
        final Document doc = document.asDocument();

        if (doc.contains(ID_FIELD_NAME)) {
            updateAsync(new LongToIntCallback(results), BuilderFactory.start()
                    .add(doc.get(ID_FIELD_NAME)), doc, false, true, durability);
        }
        else {
            insertAsync(results, durability, doc);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@code collStats} command to the MongoDB server.
     * </p>
     * 
     * @see MongoCollection#stats
     */
    @Override
    public Document stats() throws MongoDbException {
        return myDatabase.runCommand("collStats", getName(), null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server and setup the
     * streaming query callback.
     * </p>
     */
    @Override
    public void streamingFind(final Callback<Document> results, final Find query)
            throws MongoDbException {
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

        final Query queryMessage = new Query(getDatabaseName(), myName,
                queryDoc, query.getReturnFields(), query.getBatchSize(),
                query.getLimit(), query.getNumberToSkip(),
                query.isTailable() /* tailable */, readPreference,
                false /* noCursorTimeout */,
                query.isTailable() /* awaitData */, false /* exhaust */,
                query.isPartialOk());

        final QueryStreamingCallback callback = new QueryStreamingCallback(
                myClient, queryMessage, results);
        final String address = myClient.send(queryMessage, callback);

        callback.setAddress(address);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send an {@link Update} message to the server.
     * </p>
     */
    @Override
    public void updateAsync(final Callback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException {

        final Update updateMessage = new Update(getDatabaseName(), myName,
                query.asDocument(), update.asDocument(), multiUpdate, upsert);

        if (Durability.NONE == durability) {
            myClient.send(updateMessage, null);
            results.callback(Long.valueOf(-1));
        }
        else {
            myClient.send(updateMessage, asGetLastError(durability),
                    new ReplyLongCallback(results));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@code collMod} command to the server.
     * </p>
     */
    @Override
    public Document updateOptions(final DocumentAssignable options)
            throws MongoDbException {
        return myDatabase
                .runCommand("collMod", getName(), options.asDocument());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@code validate} command to the server.
     * </p>
     */
    @Override
    public Document validate(final ValidateMode mode) throws MongoDbException {
        Document result = null;

        switch (mode) {
        case INDEX_ONLY:
            result = myDatabase.runCommand("validate", getName(),
                    BuilderFactory.start().add("scandata", false).build());
            break;
        case NORMAL:
            result = myDatabase.runCommand("validate", getName(), null);
            break;
        case FULL:
            result = myDatabase.runCommand("validate", getName(),
                    BuilderFactory.start().add("full", true).build());
            break;
        }

        return result;
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
}
