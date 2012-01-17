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
import com.allanbank.mongodb.bson.element.IntegerElement;
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

        myClient.send(insertMessage, asGetLastError(durability), new NCallback(
                results));
        if (Durability.NONE.equals(durability)) {
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

        if (Durability.NONE.equals(durability)) {
            myClient.send(updateMessage);
            results.callback(Integer.valueOf(-1));
        }
        else {
            myClient.send(updateMessage, asGetLastError(durability),
                    new NCallback(results));
        }
    }
}
