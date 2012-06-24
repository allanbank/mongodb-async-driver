/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Future;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.commands.Distinct;
import com.allanbank.mongodb.commands.Find;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.GroupBy;
import com.allanbank.mongodb.commands.MapReduce;

/**
 * Interface for interacting with a MongoDB collection.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoCollection {
    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * <p>
     * This is equivalent to calling {@link #countAsync(Document)
     * countAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query document.
     * @return The number of matching documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count(Document query) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param query
     *            The query document.
     * @param replicaOk
     *            If true, then the query can be run against a replica which
     *            might be slightly behind the primary.
     * @return The number of matching documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count(Document query, boolean replicaOk)
            throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param results
     *            The callback to notify of the results.
     * @param query
     *            The query document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void countAsync(Callback<Long> results, Document query)
            throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
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
    public void countAsync(Callback<Long> results, Document query,
            boolean replicaOk) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param query
     *            The query document.
     * @return A future that will be updated with the number of matching
     *         documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<Long> countAsync(Document query) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param query
     *            The query document.
     * @param replicaOk
     *            If true, then the query can be run against a replica which
     *            might be slightly behind the primary.
     * @return A future that will be updated with the number of matching
     *         documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<Long> countAsync(Document query, boolean replicaOk)
            throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified
     * allowing duplicate entries.
     * 
     * @param keys
     *            The keys to use for the index.
     * @throws MongoDbException
     *             On a failure building the index.
     */
    public void createIndex(LinkedHashMap<String, Integer> keys)
            throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified and if
     * <tt>unique</tt> is true ensuring entries are unique.
     * 
     * @param keys
     *            The keys to use for the index.
     * @param unique
     *            If true then the index created will enforce entries are
     *            unique.
     * @throws MongoDbException
     *             On a failure building the index.
     */
    public void createIndex(LinkedHashMap<String, Integer> keys, boolean unique)
            throws MongoDbException;

    /**
     * Creates an index with the specified name, across the keys specified and
     * if <tt>unique</tt> is true ensuring entries are unique.
     * 
     * @param name
     *            The name of the index. If <code>null</code> then a name is
     *            generated based on the keys.
     * @param keys
     *            The keys to use for the index.
     * @param unique
     *            If true then the index created will enforce entries are
     *            unique.
     * @throws MongoDbException
     *             On a failure building the index.
     */
    public void createIndex(String name, LinkedHashMap<String, Integer> keys,
            boolean unique) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @return The results of the delete. If the durability of the operation is
     *         NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public long delete(Document query) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     * @return The results of the delete. If the durability of the operation is
     *         NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public long delete(Document query, boolean singleDelete)
            throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     * @param durability
     *            The durability for the delete.
     * @return The results of the delete. If the durability of the operation is
     *         NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public long delete(Document query, boolean singleDelete,
            Durability durability) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param durability
     *            The durability for the delete.
     * @return The results of the delete. If the durability of the operation is
     *         NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public long delete(Document query, Durability durability)
            throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param results
     *            Callback that will be notified of the results of the query. If
     *            the durability of the operation is NONE then this will be -1.
     * @param query
     *            Query to locate the documents to be deleted.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public void deleteAsync(Callback<Long> results, Document query)
            throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
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
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public void deleteAsync(Callback<Long> results, Document query,
            boolean singleDelete) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
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
     */
    public void deleteAsync(Callback<Long> results, Document query,
            boolean singleDelete, Durability durability)
            throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param results
     *            Callback that will be notified of the results of the query. If
     *            the durability of the operation is NONE then this will be -1.
     * @param query
     *            Query to locate the documents to be deleted.
     * @param durability
     *            The durability for the delete.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public void deleteAsync(Callback<Long> results, Document query,
            Durability durability) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @return Future that will be updated with the results of the delete. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public Future<Long> deleteAsync(Document query) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     * @return Future that will be updated with the results of the delete. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public Future<Long> deleteAsync(Document query, boolean singleDelete)
            throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param singleDelete
     *            If true then only a single document will be deleted. If
     *            running in a sharded environment then this field must be false
     *            or the query must contain the shard key.
     * @param durability
     *            The durability for the delete.
     * @return Future that will be updated with the results of the delete. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public Future<Long> deleteAsync(Document query, boolean singleDelete,
            Durability durability) throws MongoDbException;

    /**
     * Deletes a set of documents matching a query from the collection.
     * 
     * @param query
     *            Query to locate the documents to be deleted.
     * @param durability
     *            The durability for the delete.
     * @return Future that will be updated with the results of the delete. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error deleting the documents.
     */
    public Future<Long> deleteAsync(Document query, Durability durability)
            throws MongoDbException;

    /**
     * Invokes a distinct command on the server.
     * 
     * @param command
     *            The details of the distinct request.
     * @return The distinct results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public ArrayElement distinct(Distinct command) throws MongoDbException;

    /**
     * Invokes a distinct command on the server.
     * 
     * @param results
     *            Callback for the distinct results returned.
     * @param command
     *            The details of the distinct request.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void distinctAsync(Callback<ArrayElement> results, Distinct command)
            throws MongoDbException;

    /**
     * Invokes a distinct command on the server.
     * 
     * @param command
     *            The details of the distinct request.
     * @return Future for the distinct results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<ArrayElement> distinctAsync(Distinct command)
            throws MongoDbException;

    /**
     * Drops the collection from the database.
     * 
     * @return True if the collection was successfully dropped.
     * @throws MongoDbException
     *             On an error dropping the collection.
     */
    public boolean drop() throws MongoDbException;

    /**
     * Deletes the indexes matching the keys specified.
     * 
     * @param keys
     *            The keys for the index to be dropped.
     * @return If any indexes were removed.
     * @throws MongoDbException
     *             On an error deleting the indexes.
     */
    public boolean dropIndex(LinkedHashMap<String, Integer> keys)
            throws MongoDbException;

    /**
     * Deletes the indexes with the provided name.
     * 
     * @param name
     *            The name of the index.
     * @return If any indexes were removed.
     * @throws MongoDbException
     *             On an error deleting the indexes.
     */
    public boolean dropIndex(String name) throws MongoDbException;

    /**
     * Finds the set of documents matching the query document in the collection.
     * <p>
     * This is equivalent to calling {@link #findAsync(Document)
     * findAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query document.
     * @return The ClosableIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public ClosableIterator<Document> find(Document query)
            throws MongoDbException;

    /**
     * Finds the set of documents matching the query in the collection.
     * <p>
     * This is equivalent to calling {@link #findAsync(Find)
     * findAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The ClosableIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public ClosableIterator<Document> find(Find query) throws MongoDbException;

    /**
     * Invokes a findAndModify command on the server. The <tt>query</tt> is used
     * to locate a document to apply a set of <tt>update</tt>s to.
     * 
     * @param command
     *            The details of the find and modify request.
     * @return The found document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Document findAndModify(FindAndModify command)
            throws MongoDbException;

    /**
     * Invokes a findAndModify command on the server. The <tt>query</tt> is used
     * to locate a document to apply a set of <tt>update</tt>s to.
     * 
     * @param results
     *            Callback for the the found document.
     * @param command
     *            The details of the find and modify request.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void findAndModifyAsync(Callback<Document> results,
            FindAndModify command) throws MongoDbException;

    /**
     * Invokes a findAndModify command on the server. The <tt>query</tt> is used
     * to locate a document to apply a set of <tt>update</tt>s to.
     * 
     * @param command
     *            The details of the find and modify request.
     * @return Future for the found document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<Document> findAndModifyAsync(FindAndModify command)
            throws MongoDbException;

    /**
     * Finds the set of documents matching the query document in the collection.
     * 
     * @param results
     *            Callback that will be notified of the results of the query.
     * @param query
     *            The query document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void findAsync(Callback<ClosableIterator<Document>> results,
            Document query) throws MongoDbException;

    /**
     * Finds the set of documents matching the query in the collection.
     * 
     * @param results
     *            Callback that will be notified of the results of the query.
     * @param query
     *            The query details.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void findAsync(Callback<ClosableIterator<Document>> results,
            Find query) throws MongoDbException;

    /**
     * Finds the set of documents matching the query document in the collection.
     * 
     * @param query
     *            The query document.
     * @return A future for the ClosableIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<ClosableIterator<Document>> findAsync(Document query)
            throws MongoDbException;

    /**
     * Finds the set of documents matching the query in the collection.
     * 
     * @param query
     *            The query details.
     * @return A future for the ClosableIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<ClosableIterator<Document>> findAsync(Find query)
            throws MongoDbException;

    /**
     * Finds a single matching document in the collection.
     * 
     * @param query
     *            The query document.
     * @return The first found document.
     * @throws MongoDbException
     *             On an error finding the document.
     */
    public Document findOne(Document query) throws MongoDbException;

    /**
     * Finds a single matching document in the collection.
     * 
     * @param results
     *            Callback that will be notified of the results of the query.
     * @param query
     *            The query document.
     * @throws MongoDbException
     *             On an error finding the document.
     */
    public void findOneAsync(Callback<Document> results, Document query)
            throws MongoDbException;

    /**
     * Finds a single matching document in the collection.
     * 
     * @param query
     *            The query document.
     * @return The first found document.
     * @throws MongoDbException
     *             On an error finding the document.
     */
    public Future<Document> findOneAsync(Document query)
            throws MongoDbException;

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getDatabaseName();

    /**
     * Returns the name of the collection.
     * 
     * @return The name of the collection.
     */
    public String getName();

    /**
     * Invokes a group command on the server.
     * 
     * @param command
     *            The details of the group request.
     * @return The group results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public ArrayElement groupBy(GroupBy command) throws MongoDbException;

    /**
     * Invokes a group command on the server.
     * 
     * @param results
     *            Callback for the group results returned.
     * @param command
     *            The details of the group request.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void groupByAsync(Callback<ArrayElement> results, GroupBy command)
            throws MongoDbException;

    /**
     * Invokes a group command on the server.
     * 
     * @param command
     *            The details of the group request.
     * @return Future for the group results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<ArrayElement> groupByAsync(GroupBy command)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, Document[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param documents
     *            The documents to add to the collection.
     * @return The number of documents inserted. If the durability is NONE then
     *         this value will be <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final boolean continueOnError, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, Document[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @return The number of documents inserted. If the durability is NONE then
     *         this value will be <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final boolean continueOnError,
            final Durability durability, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, Document[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param documents
     *            The documents to add to the collection.
     * @return The number of documents inserted. If the durability is NONE then
     *         this value will be <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(Document... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, Document[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @return The number of documents inserted. If the durability is NONE then
     *         this value will be <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final Durability durability, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param documents
     *            The documents to add to the collection.
     * @return Future that will be updated with the results of the insert. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public Future<Integer> insertAsync(boolean continueOnError,
            Document... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @return Future that will be updated with the results of the insert. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public Future<Integer> insertAsync(boolean continueOnError,
            Durability durability, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. If the durability of the operation is NONE then this
     *            will be -1.
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public void insertAsync(Callback<Integer> results, boolean continueOnError,
            Document... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. If the durability of the operation is NONE then this
     *            will be -1.
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public void insertAsync(Callback<Integer> results, boolean continueOnError,
            Durability durability, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. If the durability of the operation is NONE then this
     *            will be -1.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public void insertAsync(Callback<Integer> results, Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param results
     *            {@link Callback} that will be notified with the results of the
     *            insert. If the durability of the operation is NONE then this
     *            will be -1.
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public void insertAsync(Callback<Integer> results, Durability durability,
            Document... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param documents
     *            The documents to add to the collection.
     * @return Future that will be updated with the results of the insert. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public Future<Integer> insertAsync(Document... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * 
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @return Future that will be updated with the results of the insert. If
     *         the durability of the operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public Future<Integer> insertAsync(Durability durability,
            Document... documents) throws MongoDbException;

    /**
     * Invokes a mapReduce command on the server.
     * 
     * @param command
     *            The details of the map/reduce request.
     * @return The map/reduce results returned. Note this might be empty if the
     *         output type is not inline.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public List<Document> mapReduce(MapReduce command) throws MongoDbException;

    /**
     * Invokes a mapReduce command on the server.
     * 
     * @param results
     *            Callback for the map/reduce results returned. Note this might
     *            be empty if the output type is not inline.
     * @param command
     *            The details of the map/reduce request.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public void mapReduceAsync(Callback<List<Document>> results,
            MapReduce command) throws MongoDbException;

    /**
     * Invokes a mapReduce command on the server.
     * 
     * @param command
     *            The details of the map/reduce request.
     * @return Future for the map/reduce results returned. Note this might be
     *         empty if the output type is not inline.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Future<List<Document>> mapReduceAsync(MapReduce command)
            throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @return The number of documents updated. If the durability of the
     *         operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public long update(Document query, Document update) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
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
     * @return The number of documents updated. If the durability of the
     *         operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public long update(Document query, Document update,
            final boolean multiUpdate, final boolean upsert)
            throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
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
     *            The durability for the insert.
     * @return The number of documents updated. If the durability of the
     *         operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public long update(Document query, Document update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @param durability
     *            The durability for the update.
     * @return The number of documents updated. If the durability of the
     *         operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public long update(Document query, Document update,
            final Durability durability) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param results
     *            The {@link Callback} that will be notified of the number of
     *            documents updated. If the durability of the operation is NONE
     *            then this will be -1.
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public void updateAsync(Callback<Long> results, Document query,
            Document update) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
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
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public void updateAsync(Callback<Long> results, Document query,
            Document update, boolean multiUpdate, boolean upsert)
            throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
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
     */
    public void updateAsync(Callback<Long> results, Document query,
            Document update, boolean multiUpdate, boolean upsert,
            Durability durability) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param results
     *            The {@link Callback} that will be notified of the number of
     *            documents updated. If the durability of the operation is NONE
     *            then this will be -1.
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @param durability
     *            The durability for the update.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public void updateAsync(Callback<Long> results, Document query,
            Document update, Durability durability) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @return A {@link Future} that will be updated with the number of
     *         documents updated. If the durability of the operation is NONE
     *         then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public Future<Long> updateAsync(Document query, Document update)
            throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
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
     * @return A {@link Future} that will be updated with the number of
     *         documents updated. If the durability of the operation is NONE
     *         then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public Future<Long> updateAsync(Document query, Document update,
            boolean multiUpdate, boolean upsert) throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
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
     * @return A {@link Future} that will be updated with the number of
     *         documents updated. If the durability of the operation is NONE
     *         then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public Future<Long> updateAsync(Document query, Document update,
            boolean multiUpdate, boolean upsert, Durability durability)
            throws MongoDbException;

    /**
     * Applies updates to a set of documents within the collection. The
     * documents to update are selected by the <tt>query</tt> and the updates
     * are describe by the <tt>update</tt> document.
     * 
     * @param query
     *            The query to select the documents to update.
     * @param update
     *            The updates to apply to the selected documents.
     * @param durability
     *            The durability for the update.
     * @return A {@link Future} that will be updated with the number of
     *         documents updated. If the durability of the operation is NONE
     *         then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public Future<Long> updateAsync(Document query, Document update,
            Durability durability) throws MongoDbException;
}
