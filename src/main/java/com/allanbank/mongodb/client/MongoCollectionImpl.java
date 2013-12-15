/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
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
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.Text;
import com.allanbank.mongodb.builder.TextResult;
import com.allanbank.mongodb.client.callback.CursorCallback;
import com.allanbank.mongodb.client.callback.CursorStreamingCallback;
import com.allanbank.mongodb.client.callback.LongToIntCallback;
import com.allanbank.mongodb.client.callback.ReplyArrayCallback;
import com.allanbank.mongodb.client.callback.ReplyDocumentCallback;
import com.allanbank.mongodb.client.callback.ReplyIntegerCallback;
import com.allanbank.mongodb.client.callback.ReplyLongCallback;
import com.allanbank.mongodb.client.callback.ReplyResultCallback;
import com.allanbank.mongodb.client.callback.SingleDocumentCallback;
import com.allanbank.mongodb.client.callback.TextCallback;
import com.allanbank.mongodb.client.message.AggregateCommand;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.util.FutureUtils;

/**
 * Implementation of the {@link MongoCollection} interface.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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
    public void aggregateAsync(final Callback<MongoIterator<Document>> results,
            final Aggregate command) throws MongoDbException {

        final AggregateCommand commandMsg = toCommand(command, false);

        final CursorCallback callback = new CursorCallback(myClient,
                commandMsg, true, results);
        final String address = myClient.send(commandMsg, callback);

        callback.setAddress(address);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>count</code> method that implementations must
     * override.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results, final Count count)
            throws MongoDbException {
        Version minVersion = null;
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("count", getName());
        builder.addDocument("query", count.getQuery());
        if (count.getMaximumTimeMilliseconds() > 0) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("maxTimeMS", count.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference finalPreference = updateReadPreference(builder,
                count.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), finalPreference, minVersion);

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

        final MongoCollectionImpl indexCollection = new MongoCollectionImpl(
                myClient, myDatabase, "system.indexes");
        final Document indexDocument = indexEntryBuilder.build();
        if (indexCollection.findOne(indexDocument) == null) {

            final Version requiredServerVersion = determineIndexServerVersion(keys);

            final FutureCallback<Integer> callback = new FutureCallback<Integer>();
            indexCollection.doInsertAsync(callback, false, Durability.ACK,
                    requiredServerVersion, indexDocument);

            FutureUtils.unwrap(callback);
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

        Version minVersion = null;

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("distinct", getName());
        builder.addString("key", command.getKey());
        if (command.getQuery() != null) {
            builder.addDocument("query", command.getQuery());
        }
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference, minVersion);

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
     * Overridden to send a {@link AggregateCommand} message to the server to
     * explain the {@link Aggregate}.
     * </p>
     */
    @Override
    public void explainAsync(final Callback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        final AggregateCommand commandMsg = toCommand(aggregation, true);

        myClient.send(commandMsg, new SingleDocumentCallback(results));
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
                queryDoc, query.getProjection(), query.getBatchSize(),
                query.getLimit(), query.getNumberToSkip(),
                false /* tailable */, readPreference,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, query.isPartialOk());

        myClient.send(queryMessage, new SingleDocumentCallback(results));
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
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Must be the primary since this is a write.
        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), ReadPreference.PRIMARY, minVersion);
        myClient.send(commandMsg, new ReplyDocumentCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server.
     * </p>
     */
    @Override
    public void findAsync(final Callback<MongoIterator<Document>> results,
            final Find query) throws MongoDbException {

        final Query queryMessage = createQuery(query, query.getLimit(),
                query.getBatchSize(), query.isTailable(), query.isAwaitData(),
                query.isImmortalCursor());

        final CursorCallback callback = new CursorCallback(myClient,
                queryMessage, false, results);
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
    public void findOneAsync(final Callback<Document> results, final Find query)
            throws MongoDbException {
        final Query queryMessage = createQuery(query, 1, 1, false, false, false);

        myClient.send(queryMessage, new SingleDocumentCallback(results));
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
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            groupDocBuilder.add("maxTimeMS",
                    command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(
                groupDocBuilder, command.getReadPreference(), false);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference, minVersion);
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

        doInsertAsync(results, continueOnError, durability, null, documents);
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
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
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

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference, minVersion);
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
     * This is the canonical <code>stream(Aggregation)</code> method that
     * implementations must override.
     * </p>
     * 
     * @see MongoCollection#stream(StreamCallback, Aggregate)
     */
    @Override
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        final AggregateCommand commandMsg = toCommand(aggregation, false);

        final CursorStreamingCallback callback = new CursorStreamingCallback(
                myClient, commandMsg, true, results);
        final String address = myClient.send(commandMsg, callback);

        callback.setAddress(address);

        return callback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server and setup the
     * streaming query callback.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Find query) throws MongoDbException {
        final Query queryMessage = createQuery(query, query.getLimit(),
                query.getBatchSize(), query.isTailable(), query.isAwaitData(),
                query.isImmortalCursor());

        final CursorStreamingCallback callback = new CursorStreamingCallback(
                myClient, queryMessage, false, results);
        final String address = myClient.send(queryMessage, callback);

        callback.setAddress(address);

        return callback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a text command and send it to the server.
     * </p>
     * 
     * @see MongoCollection#textSearchAsync(Callback, Text)
     */
    @Override
    public void textSearchAsync(final Callback<List<TextResult>> results,
            final Text command) throws MongoDbException {
        Version minVersion = Text.REQUIRED_VERSION;
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
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.build(), readPreference, minVersion);
        myClient.send(commandMsg, new ReplyResultCallback(new TextCallback(
                results)));
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
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("collMod", getName());
        addOptions("collMod", options, commandDoc);

        myDatabase.runCommandAsync(future, commandDoc.build(),
                Version.VERSION_2_2);

        return FutureUtils.unwrap(future);
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
     * Adds the options to the document builder.
     * 
     * @param command
     *            The command to make sure is removed from the options.
     * @param options
     *            The options to be added. May be <code>null</code>.
     * @param builder
     *            The builder to add the options to.
     */
    protected void addOptions(final String command,
            final DocumentAssignable options, final DocumentBuilder builder) {
        if (options != null) {
            for (final Element element : options.asDocument()) {
                if (!command.equals(element.getName())) {
                    builder.add(element);
                }
            }
        }
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
     * Determines the minimum server version required to support the provided
     * index keys and options.
     * 
     * @param keys
     *            The index keys.
     * @return The version required for the index. May be null.
     */
    protected Version determineIndexServerVersion(final Element[] keys) {
        Version result = null;

        for (final Element key : keys) {
            if (key.getType() == ElementType.STRING) {
                final String type = key.getValueAsString();
                if (Index.GEO_2DSPHERE_INDEX_NAME.equals(type)) {
                    result = Version.later(result, Version.VERSION_2_4);
                }
                else if (Index.HASHED_INDEX_NAME.equals(type)) {
                    result = Version.later(result, Version.VERSION_2_4);
                }
                else if (Index.TEXT_INDEX_NAME.equals(type)) {
                    result = Version.later(result, Version.VERSION_2_4);
                }
            }
        }

        return result;
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
                docs, continueOnError, requiredServerVersion);
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
        Version minVersion = Aggregate.REQUIRED_VERSION;

        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("aggregate", getName());

        // Pipeline of operations.
        final ArrayBuilder pipeline = builder.pushArray("pipeline");
        for (final Element e : command.getPipeline()) {
            pipeline.add(e);
        }

        // Options - 2.6 on...
        if (command.getMaximumTimeMilliseconds() > 0) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("maxTimeMS", command.getMaximumTimeMilliseconds());
        }
        if (command.isUseCursor()) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            final DocumentBuilder cursor = builder.push("cursor");
            if (command.getBatchSize() > 0) {
                cursor.add("batchSize", command.getBatchSize());
            }
            if (command.isAllowDiskUsage()) {
                cursor.add("allowDiskUsage", true);
            }
        }
        if (explain) {
            minVersion = Version.later(minVersion, Version.VERSION_2_6);
            builder.add("explain", true);
        }

        // Should be last since might wrap command in a $query element.
        final ReadPreference readPreference = updateReadPreference(builder,
                command.getReadPreference(), true);

        final AggregateCommand commandMsg = new AggregateCommand(command,
                getDatabaseName(), getName(), builder.build(), readPreference,
                minVersion);
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
}
