/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.MapReduce;
import com.allanbank.mongodb.connection.messsage.Command;
import com.allanbank.mongodb.connection.messsage.Delete;
import com.allanbank.mongodb.connection.messsage.Insert;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Update;

/**
 * Implementation of the {@link MongoCollection} interface.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoCollectionClient extends AbstractMongoCollection {

    /**
     * Create a new MongoDatabaseClient.
     * 
     * @param client
     *            The client for interacting with MongoDB.
     * @param database
     *            The database the collection is a part of.
     * @param database
     *            The database we interact with.
     * @param name
     *            The name of the collection we interact with.
     */
    public MongoCollectionClient(final Client client,
            final MongoDatabase database, final String name) {
        super(client, database, name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@link Delete} message to the server.
     * </p>
     */
    @Override
    public void deleteAsync(final Callback<Integer> results,
            final Document query, final boolean singleDelete,
            final Durability durability) throws MongoDbException {
        final Delete deleteMessage = new Delete(getDatabaseName(), myName,
                query, singleDelete);

        if (Durability.NONE.equals(durability)) {
            myClient.send(deleteMessage);
            results.callback(Integer.valueOf(-1));
        }
        else {
            myClient.send(deleteMessage, asGetLastError(durability),
                    new NCallback(results));
        }
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
        final List<IntegerElement> okElem = result.queryPath(
                IntegerElement.class, "ok");

        return ((okElem.size() > 0) && (okElem.get(0).getValue() > 0));
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
        if (command.getQuery() != null) {
            builder.addDocument("query", command.getQuery());
        }
        if (command.getSort() != null) {
            builder.addDocument("sort", command.getSort());
        }
        if (command.getUpdate() != null) {
            builder.addDocument("update", command.getUpdate());
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
        myClient.send(commandMsg, new ReplyValueDocumentCallback(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>mapReduce</code> method that implementations
     * must override.
     * </p>
     * 
     * @see MongoCollection#mapReduceAsync(Callback, MapReduce)
     */
    @Override
    public void mapReduceAsync(Callback<List<Document>> results,
            MapReduce command) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.addString("mapReduce", getName());
        if (command.getMap() != null) {
            builder.addJavaScript("map", command.getMap());
        }
        if (command.getReduce() != null) {
            builder.addJavaScript("reduce", command.getReduce());
        }
        if (command.getFinalize() != null) {
            builder.addJavaScript("finalize", command.getFinalize());
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

        DocumentBuilder outputBuilder = builder.push("out");
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
     * Overridden to send a {@link Query} message to the server.
     * </p>
     */
    @Override
    public void findAsync(final Callback<Iterator<Document>> results,
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
     * Overridden to send an {@link Insert} message to the server.
     * </p>
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final Document... documents) throws MongoDbException {
        final Insert insertMessage = new Insert(getDatabaseName(), myName,
                Arrays.asList(documents), continueOnError);

        if (Durability.NONE == durability) {
            myClient.send(insertMessage);
            results.callback(Integer.valueOf(-1));
        }
        else {
            myClient.send(insertMessage, asGetLastError(durability),
                    new NCallback(results));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send an {@link Update} message to the server.
     * </p>
     */
    @Override
    public void updateAsync(final Callback<Integer> results,
            final Document query, final Document update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException {
        final Update updateMessage = new Update(getDatabaseName(), myName,
                query, update, multiUpdate, upsert);

        if (Durability.NONE == durability) {
            myClient.send(updateMessage);
            results.callback(Integer.valueOf(-1));
        }
        else {
            myClient.send(updateMessage, asGetLastError(durability),
                    new NCallback(results));
        }
    }
}
