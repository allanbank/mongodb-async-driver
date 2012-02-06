/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.commands.Distinct;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.GroupBy;
import com.allanbank.mongodb.commands.MapReduce;
import com.allanbank.mongodb.connection.messsage.Command;
import com.allanbank.mongodb.connection.messsage.Delete;
import com.allanbank.mongodb.connection.messsage.Insert;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Update;

/**
 * Implementation of the {@link MongoCollection} interface.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoCollectionImpl extends AbstractMongoCollection {

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
     * This is the canonical <code>count</code> method that implementations must
     * override.
     * </p>
     * 
     * @param results
     *            The callback to notify of the results.
     * @param query
     *            The query document.
     * @param replicaOk
     *            If true, then the query can be run against a replica which
     *            might be slightly behind the primary.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    @Override
    public void countAsync(final Callback<Long> results, final Document query,
            final boolean replicaOk) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("count", getName());
        builder.addDocument("query", query);

        final Command commandMsg = new Command(getDatabaseName(),
                builder.get(), replicaOk);

        myClient.send(commandMsg, new ReplyLongCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to insert the index document into the 'system.indexes'
     * collection.
     * </p>
     */
    @Override
    public void createIndex(final String name,
            final LinkedHashMap<String, Integer> keys, final boolean unique)
            throws MongoDbException {

        String indexName = name;
        if ((name == null) || name.isEmpty()) {
            final StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(myName.replace(' ', '_'));
            for (final Map.Entry<String, Integer> key : keys.entrySet()) {
                nameBuilder.append('_');
                nameBuilder.append(key.getKey().replace(' ', '_'));
                nameBuilder.append(key.getValue().toString());
            }
            indexName = nameBuilder.toString();
        }

        final DocumentBuilder indexEntryBuilder = BuilderFactory.start();
        indexEntryBuilder.addString("name", indexName);
        indexEntryBuilder.addString("ns", getDatabaseName() + "." + getName());
        if (unique) {
            indexEntryBuilder.addBoolean("unique", unique);
        }

        final DocumentBuilder keyBuilder = indexEntryBuilder.push("key");
        for (final Map.Entry<String, Integer> key : keys.entrySet()) {
            keyBuilder.addInteger(key.getKey(), key.getValue().intValue());
        }

        final MongoCollection indexCollection = new MongoCollectionImpl(
                myClient, myDatabase, "system.indexes");
        final Document indexDocument = indexEntryBuilder.get();
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
    public void deleteAsync(final Callback<Long> results, final Document query,
            final boolean singleDelete, final Durability durability)
            throws MongoDbException {
        final Delete deleteMessage = new Delete(getDatabaseName(), myName,
                query, singleDelete);

        if (Durability.NONE.equals(durability)) {
            myClient.send(deleteMessage);
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

        final Command commandMsg = new Command(getDatabaseName(), builder.get());

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
        final List<NumericElement> okElem = result.queryPath(
                NumericElement.class, "ok");

        return ((okElem.size() > 0) && (okElem.get(0).getIntValue() > 0));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a { "deleteIndexes" : <collection_name>, name :
     * <namePattern> } command.
     * </p>
     */
    @Override
    public boolean dropIndex(final Pattern namePattern) throws MongoDbException {

        final DocumentBuilder options = BuilderFactory.start();
        options.addRegularExpression("name", namePattern.pattern(), "");

        final Document result = myDatabase.runCommand("deleteIndexes", myName,
                options.get());
        final List<NumericElement> okElem = result.queryPath(
                NumericElement.class, "ok");

        return ((okElem.size() > 0) && (okElem.get(0).getIntValue() > 0));
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
        builder.addDocument("update", command.getUpdate());
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

        final Command commandMsg = new Command(getDatabaseName(), builder.get());
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
            final Document query, final Document returnFields,
            final int numberToReturn, final int numberToSkip,
            final boolean replicaOk, final boolean partial)
            throws MongoDbException {

        final Query queryMessage = new Query(getDatabaseName(), myName, query,
                returnFields, numberToReturn, numberToSkip,
                false /* tailable */, replicaOk, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, partial);

        myClient.send(queryMessage, new QueryCallback(myClient, queryMessage,
                results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Query} message to the server.
     * </p>
     * 
     * @see MongoCollection#findOneAsync(Callback, Document)
     */
    @Override
    public void findOneAsync(final Callback<Document> results,
            final Document query) throws MongoDbException {
        final Query queryMessage = new Query(getDatabaseName(), myName, query,
                null, 1 /* numberToReturn */, 0 /* skip */,
                false /* tailable */, false /* replicaOk */,
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
            groupDocBuilder.addJavaScript("$finalize",
                    command.getFinalizeFunction());
        }
        if (command.getQuery() != null) {
            groupDocBuilder.addDocument("cond", command.getQuery());
        }

        final Command commandMsg = new Command(getDatabaseName(), builder.get());
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
            final Document... documents) throws MongoDbException {
        final Insert insertMessage = new Insert(getDatabaseName(), myName,
                Arrays.asList(documents), continueOnError);

        // Make sure the documents have an _id.
        for (final Document doc : documents) {
            if (!doc.contains("_id")) {
                doc.injectId();
            }
        }

        if (Durability.NONE == durability) {
            myClient.send(insertMessage);
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
     * Overridden to construct a mapReduce command and send it to the server.
     * </p>
     * 
     * @see MongoCollection#mapReduceAsync(Callback, MapReduce)
     */
    @Override
    public void mapReduceAsync(final Callback<List<Document>> results,
            final MapReduce command) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("mapReduce", getName());
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

        final Command commandMsg = new Command(getDatabaseName(), builder.get());
        myClient.send(commandMsg, new MapReduceReplyCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send an {@link Update} message to the server.
     * </p>
     */
    @Override
    public void updateAsync(final Callback<Long> results, final Document query,
            final Document update, final boolean multiUpdate,
            final boolean upsert, final Durability durability)
            throws MongoDbException {
        final Update updateMessage = new Update(getDatabaseName(), myName,
                query, update, multiUpdate, upsert);

        if (Durability.NONE == durability) {
            myClient.send(updateMessage);
            results.callback(Long.valueOf(-1));
        }
        else {
            myClient.send(updateMessage, asGetLastError(durability),
                    new ReplyLongCallback(results));
        }
    }
}
