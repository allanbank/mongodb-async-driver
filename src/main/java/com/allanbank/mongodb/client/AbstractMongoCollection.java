/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.commands.Distinct;
import com.allanbank.mongodb.commands.Find;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.GroupBy;
import com.allanbank.mongodb.commands.MapReduce;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.GetLastError;

/**
 * Helper class for forward all methods to the canonical version (which is
 * abstract in this class).
 * <p>
 * This class keeps the clutter in the derived class to a minimum and also deals
 * with the conversion of the asynchronous method invocations into synchronous
 * methods for those uses cases that do not require an asynchronous interface.
 * </p>
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMongoCollection extends AbstractMongo implements
        MongoCollection {

    /**
     * The default for if a delete should only delete the first document it
     * matches.
     */
    public static final boolean DELETE_SINGLE_DELETE_DEFAULT = false;

    /** The default for if an insert should continue on an error. */
    public static final boolean INSERT_CONTINUE_ON_ERROR_DEFAULT = false;

    /** The default for using a replica on a query. */
    public static final boolean REPLICA_OK_DEFAULT = false;

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

    /**
     * Create a new AbstractMongoCollection.
     * 
     * @param client
     *            The client for interacting with MongoDB.
     * @param database
     *            The database we interact with.
     * @param name
     *            The name of the collection we interact with.
     */
    public AbstractMongoCollection(final Client client,
            final MongoDatabase database, final String name) {
        super();

        myClient = client;
        myDatabase = database;
        myName = name;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #count(Document, boolean)} method with
     * {@value #REPLICA_OK_DEFAULT} as the <tt>replicaOk</tt> argument.
     * </p>
     * 
     * @param query
     *            The query document.
     * @return The number of matching documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    @Override
    public long count(final Document query) throws MongoDbException {
        return count(query, REPLICA_OK_DEFAULT);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Document, boolean)} method.
     * </p>
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
    @Override
    public long count(final Document query, final boolean replicaOk)
            throws MongoDbException {

        final Future<Long> future = countAsync(query, replicaOk);

        return unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Document, boolean)}
     * method with {@value #REPLICA_OK_DEFAULT} as the <tt>replicaOk</tt>
     * argument.
     * </p>
     * 
     * @param results
     *            The callback to notify of the results.
     * @param query
     *            The query document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    @Override
    public void countAsync(final Callback<Long> results, final Document query)
            throws MongoDbException {
        countAsync(results, query, REPLICA_OK_DEFAULT);
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
    public abstract void countAsync(Callback<Long> results, Document query,
            boolean replicaOk) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Document, boolean)}
     * method with {@value #REPLICA_OK_DEFAULT} as the <tt>replicaOk</tt>
     * argument.
     * </p>
     * 
     * @param query
     *            The query document.
     * @return A future that will be updated with the number of matching
     *         documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    @Override
    public Future<Long> countAsync(final Document query)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        countAsync(future, query, REPLICA_OK_DEFAULT);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Document, boolean)}
     * method.
     * </p>
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
    @Override
    public Future<Long> countAsync(final Document query, final boolean replicaOk)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        countAsync(future, query, replicaOk);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #createIndex(String, LinkedHashMap, boolean)} method with
     * <code>false</code> for <tt>unique</tt>.
     * </p>
     * 
     * @see #createIndex(LinkedHashMap, boolean)
     */
    @Override
    public void createIndex(final LinkedHashMap<String, Integer> keys)
            throws MongoDbException {
        createIndex(keys, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #createIndex(String, LinkedHashMap, boolean)} method with
     * <code>null</code> for the name.
     * </p>
     * 
     * @see #createIndex(String, LinkedHashMap, boolean)
     */
    @Override
    public void createIndex(final LinkedHashMap<String, Integer> keys,
            final boolean unique) throws MongoDbException {
        createIndex(null, keys, unique);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>createIndex</code> method that
     * implementations must override.
     * </p>
     * 
     * @see MongoCollection#createIndex(String, LinkedHashMap, boolean)
     */
    @Override
    public abstract void createIndex(String name,
            LinkedHashMap<String, Integer> keys, boolean unique)
            throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #delete(Document, boolean, Durability)}
     * method with false as the <tt>singleDelete</tt> argument and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #delete(Document, boolean, Durability)
     */
    @Override
    public long delete(final Document query) throws MongoDbException {
        return delete(query, DELETE_SINGLE_DELETE_DEFAULT,
                getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #delete(Document, boolean, Durability)}
     * method with the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #delete(Document, boolean, Durability)
     */
    @Override
    public long delete(final Document query, final boolean singleDelete)
            throws MongoDbException {
        return delete(query, singleDelete, getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Document, boolean, Durability)} method.
     * </p>
     * 
     * @see #deleteAsync(Document, boolean, Durability)
     */
    @Override
    public long delete(final Document query, final boolean singleDelete,
            final Durability durability) throws MongoDbException {

        final Future<Long> future = deleteAsync(query, singleDelete, durability);

        return unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Document, boolean, Durability)} method with false as
     * the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see #delete(Document, boolean, Durability)
     */
    @Override
    public long delete(final Document query, final Durability durability)
            throws MongoDbException {
        return delete(query, DELETE_SINGLE_DELETE_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, Document, boolean, Durability)} method with
     * false as the <tt>singleDelete</tt> argument and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #deleteAsync(Callback, Document, boolean, Durability)
     */
    @Override
    public void deleteAsync(final Callback<Long> results, final Document query)
            throws MongoDbException {
        deleteAsync(results, query, DELETE_SINGLE_DELETE_DEFAULT,
                getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, Document, boolean, Durability)} method with
     * the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document)
     */
    @Override
    public void deleteAsync(final Callback<Long> results, final Document query,
            final boolean singleDelete) throws MongoDbException {
        deleteAsync(results, query, singleDelete, getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>delete</code> method that implementations
     * must override.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document, boolean, Durability)
     */
    @Override
    public abstract void deleteAsync(final Callback<Long> results,
            final Document query, final boolean singleDelete,
            final Durability durability) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, Document, boolean, Durability)} method with
     * false as the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document, boolean)
     */
    @Override
    public void deleteAsync(final Callback<Long> results, final Document query,
            final Durability durability) throws MongoDbException {
        deleteAsync(results, query, DELETE_SINGLE_DELETE_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, Document, boolean, Durability)} method with
     * false as the <tt>singleDelete</tt> argument and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document)
     */
    @Override
    public Future<Long> deleteAsync(final Document query)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        deleteAsync(future, query, DELETE_SINGLE_DELETE_DEFAULT,
                getDefaultDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, Document, boolean, Durability)} method with
     * the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document)
     */
    @Override
    public Future<Long> deleteAsync(final Document query,
            final boolean singleDelete) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        deleteAsync(future, query, singleDelete, getDefaultDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #deleteAsync(Callback, Document)} method.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document)
     */
    @Override
    public Future<Long> deleteAsync(final Document query,
            final boolean singleDelete, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        deleteAsync(future, query, singleDelete, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #deleteAsync(Callback, Document)} method
     * with false as the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, Document)
     */
    @Override
    public Future<Long> deleteAsync(final Document query,
            final Durability durability) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        deleteAsync(future, query, DELETE_SINGLE_DELETE_DEFAULT, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Distinct)}.
     * </p>
     */
    @Override
    public ArrayElement distinct(final Distinct command)
            throws MongoDbException {
        return unwrap(distinctAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>disitnct</code> method that implementations
     * must override.
     * </p>
     */
    @Override
    public abstract void distinctAsync(Callback<ArrayElement> results,
            Distinct command) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Callback, Distinct)}.
     * </p>
     */
    @Override
    public Future<ArrayElement> distinctAsync(final Distinct command)
            throws MongoDbException {
        final FutureCallback<ArrayElement> future = new FutureCallback<ArrayElement>();

        distinctAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * To generate the name of the index and then drop it.
     * </p>
     */
    @Override
    public boolean dropIndex(final LinkedHashMap<String, Integer> keys)
            throws MongoDbException {
        return dropIndex(buildIndexName(keys));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>dropIndex</code> method that implementations
     * must override.
     * </p>
     */
    @Override
    public abstract boolean dropIndex(String name) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Document)} method.
     * </p>
     * 
     * @see #findAsync(Document)
     */
    @Override
    public ClosableIterator<Document> find(final Document query)
            throws MongoDbException {
        return unwrap(findAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Find)} method.
     * </p>
     * 
     * @see #findAsync(Find)
     */
    @Override
    public ClosableIterator<Document> find(final Find query)
            throws MongoDbException {
        return unwrap(findAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAndModifyAsync(FindAndModify)}.
     * </p>
     * 
     * @see #findAndModifyAsync(FindAndModify)
     */
    @Override
    public Document findAndModify(final FindAndModify command)
            throws MongoDbException {
        return unwrap(findAndModifyAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>findAndModify</code> method that
     * implementations must override.
     * </p>
     * 
     * @see MongoCollection#findAndModifyAsync(Callback, FindAndModify)
     */
    @Override
    public abstract void findAndModifyAsync(Callback<Document> results,
            FindAndModify command) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findAndModifyAsync(Callback, FindAndModify)}.
     * </p>
     * 
     * @see #findAndModifyAsync(Callback, FindAndModify)
     */
    @Override
    public Future<Document> findAndModifyAsync(final FindAndModify command)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>();

        findAndModifyAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find)}.
     * </p>
     * 
     * @see #findAsync(Callback, Document)
     */
    @Override
    public void findAsync(final Callback<ClosableIterator<Document>> results,
            final Document query) throws MongoDbException {
        findAsync(results, new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>find</code> method that implementations must
     * override.
     * </p>
     * 
     * @see MongoCollection#findAsync(Callback, Find)
     */
    @Override
    public abstract void findAsync(
            final Callback<ClosableIterator<Document>> results, final Find query)
            throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Document)}.
     * </p>
     * 
     * @see #findAsync(Callback, Document)
     */
    @Override
    public Future<ClosableIterator<Document>> findAsync(final Document query)
            throws MongoDbException {
        final FutureCallback<ClosableIterator<Document>> future = new FutureCallback<ClosableIterator<Document>>();

        findAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find)}.
     * </p>
     * 
     * @see #findAsync(Callback, Find)
     */
    @Override
    public Future<ClosableIterator<Document>> findAsync(final Find query)
            throws MongoDbException {
        final FutureCallback<ClosableIterator<Document>> future = new FutureCallback<ClosableIterator<Document>>();

        findAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Document)}.
     * </p>
     * 
     * @see #findOneAsync(Document)
     */
    @Override
    public Document findOne(final Document query) throws MongoDbException {
        return unwrap(findOneAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>findOne</code> method that implementations
     * must override.
     * </p>
     * 
     * @see MongoCollection#findOneAsync(Callback, Document)
     */
    @Override
    public abstract void findOneAsync(Callback<Document> results, Document query)
            throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Callback, Document)}.
     * </p>
     * 
     * @see #findOneAsync(Callback, Document)
     */
    @Override
    public Future<Document> findOneAsync(final Document query)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>();

        findOneAsync(future, query);

        return future;
    }

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    @Override
    public String getDatabaseName() {
        return myDatabase.getName();
    }

    /**
     * Returns the name of the collection.
     * 
     * @return The name of the collection.
     */
    @Override
    public String getName() {
        return myName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(GroupBy)}.
     * </p>
     */
    @Override
    public ArrayElement groupBy(final GroupBy command) throws MongoDbException {
        return unwrap(groupByAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>groupBy</code> method that implementations
     * must override.
     * </p>
     */
    @Override
    public abstract void groupByAsync(Callback<ArrayElement> results,
            GroupBy command) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(Callback, GroupBy)}.
     * </p>
     */
    @Override
    public Future<ArrayElement> groupByAsync(final GroupBy command)
            throws MongoDbException {
        final FutureCallback<ArrayElement> future = new FutureCallback<ArrayElement>();

        groupByAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #insert(boolean, Durability, Document...)}
     * method with the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #insert(boolean, Durability, Document[])
     */
    @Override
    public int insert(final boolean continueOnError,
            final Document... documents) throws MongoDbException {
        return insert(continueOnError, getDefaultDurability(), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(boolean, Durability, Document...)} method.
     * </p>
     * 
     * @see #insertAsync(boolean, Durability, Document[])
     */
    @Override
    public int insert(final boolean continueOnError,
            final Durability durability, final Document... documents)
            throws MongoDbException {
        final Future<Integer> future = insertAsync(continueOnError, durability,
                documents);

        return unwrap(future).intValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #insert(boolean, Durability, Document...)}
     * method with <tt>continueOnError</tt> set to false and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(boolean, Durability, Document[])
     */
    @Override
    public int insert(final Document... documents) throws MongoDbException {
        return insert(INSERT_CONTINUE_ON_ERROR_DEFAULT, getDefaultDurability(),
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #insert(boolean, Durability, Document...)}
     * method with <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see #insert(boolean, Durability, Document[])
     */
    @Override
    public int insert(final Durability durability, final Document... documents)
            throws MongoDbException {
        return insert(INSERT_CONTINUE_ON_ERROR_DEFAULT, durability, documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public Future<Integer> insertAsync(final boolean continueOnError,
            final Document... documents) throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>();

        insertAsync(future, continueOnError, getDefaultDurability(), documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with <tt>continueOnError</tt> set to false and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public Future<Integer> insertAsync(final boolean continueOnError,
            final Durability durability, final Document... documents)
            throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>();

        insertAsync(future, continueOnError, durability, documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Document... documents)
            throws MongoDbException {
        insertAsync(results, continueOnError, getDefaultDurability(), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>insert</code> method that implementations
     * must override.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public abstract void insertAsync(final Callback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final Document... documents) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with <tt>continueOnError</tt> set to false and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final Document... documents) throws MongoDbException {
        insertAsync(results, INSERT_CONTINUE_ON_ERROR_DEFAULT,
                getDefaultDurability(), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final Durability durability, final Document... documents)
            throws MongoDbException {
        insertAsync(results, INSERT_CONTINUE_ON_ERROR_DEFAULT, durability,
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with <tt>continueOnError</tt> set to false and the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public Future<Integer> insertAsync(final Document... documents)
            throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>();

        insertAsync(future, INSERT_CONTINUE_ON_ERROR_DEFAULT,
                getDefaultDurability(), documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, Document...)} method
     * with <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      Document[])
     */
    @Override
    public Future<Integer> insertAsync(final Durability durability,
            final Document... documents) throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>();

        insertAsync(future, INSERT_CONTINUE_ON_ERROR_DEFAULT, durability,
                documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduceAsync(MapReduce)}.
     * </p>
     * 
     * @see #mapReduceAsync(MapReduce)
     */
    @Override
    public List<Document> mapReduce(final MapReduce command)
            throws MongoDbException {
        return unwrap(mapReduceAsync(command));
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
    public abstract void mapReduceAsync(Callback<List<Document>> results,
            MapReduce command) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findAndModifyAsync(Callback, FindAndModify)}.
     * </p>
     * 
     * @see #mapReduceAsync(Callback, MapReduce)
     */
    @Override
    public Future<List<Document>> mapReduceAsync(final MapReduce command)
            throws MongoDbException {
        final FutureCallback<List<Document>> future = new FutureCallback<List<Document>>();

        mapReduceAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(Document, Document, boolean, boolean, Durability)} method
     * with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #update(Document, Document, boolean, boolean, Durability)
     */
    @Override
    public long update(final Document query, final Document update)
            throws MongoDbException {
        return update(query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(Document, Document, boolean, boolean, Durability)} method
     * with the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #update(Document, Document, boolean, boolean, Durability)
     */
    @Override
    public long update(final Document query, final Document update,
            final boolean multiUpdate, final boolean upsert)
            throws MongoDbException {
        return update(query, update, multiUpdate, upsert,
                getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Document, Document, boolean, boolean, Durability)}
     * method.
     * </p>
     * 
     * @see #updateAsync(Document, Document, boolean, boolean, Durability)
     */
    @Override
    public long update(final Document query, final Document update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException {

        final Future<Long> future = updateAsync(query, update, multiUpdate,
                upsert, durability);

        return unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(Document, Document, boolean, boolean, Durability)} method
     * with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #update(Document, Document, boolean, boolean, Durability)
     */
    @Override
    public long update(final Document query, final Document update,
            final Durability durability) throws MongoDbException {
        return update(query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results, final Document query,
            final Document update) throws MongoDbException {
        updateAsync(results, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * using the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results, final Document query,
            final Document update, final boolean multiUpdate,
            final boolean upsert) throws MongoDbException {
        updateAsync(results, query, update, multiUpdate, upsert,
                getDefaultDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This is the canonical <code>update</code> method that implementations
     * must override.
     * </p>
     * 
     * @see MongoCollection#updateAsync(Callback, Document, Document, boolean,
     *      boolean, Durability)
     */
    @Override
    public abstract void updateAsync(final Callback<Long> results,
            final Document query, final Document update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results, final Document query,
            final Document update, final Durability durability)
            throws MongoDbException {
        updateAsync(results, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public Future<Long> updateAsync(final Document query, final Document update)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        updateAsync(future, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDefaultDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * using the {@link #getDefaultDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public Future<Long> updateAsync(final Document query,
            final Document update, final boolean multiUpdate,
            final boolean upsert) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        updateAsync(future, query, update, multiUpdate, upsert,
                getDefaultDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * method.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public Future<Long> updateAsync(final Document query,
            final Document update, final boolean multiUpdate,
            final boolean upsert, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        updateAsync(future, query, update, multiUpdate, upsert, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #updateAsync(Callback, Document, Document, boolean, boolean,
     *      Durability)
     */
    @Override
    public Future<Long> updateAsync(final Document query,
            final Document update, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>();

        updateAsync(future, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);

        return future;
    }

    /**
     * Converts the {@link Durability} into a {@link GetLastError} command.
     * 
     * @param durability
     *            The {@link Durability} to convert.
     * @return The {@link GetLastError} command.
     */
    protected GetLastError asGetLastError(final Durability durability) {
        return new GetLastError(getDatabaseName(), durability.isWaitForFsync(),
                durability.isWaitForJournal(), durability.getWaitForReplicas(),
                durability.getWaitTimeoutMillis());
    }

    /**
     * Generates a name for the index based on the keys.
     * 
     * @param keys
     *            The keys for the index.
     * @return The name for the index.
     */
    protected String buildIndexName(final LinkedHashMap<String, Integer> keys) {
        String indexName;
        final StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(myName.replace(' ', '_'));
        for (final Map.Entry<String, Integer> key : keys.entrySet()) {
            nameBuilder.append('_');
            nameBuilder.append(key.getKey().replace(' ', '_'));
            nameBuilder.append(key.getValue().toString());
        }
        indexName = nameBuilder.toString();
        return indexName;
    }

    /**
     * Returns the {@link Durability} from the {@link MongoDbConfiguration}.
     * 
     * @return The default durability from the {@link MongoDbConfiguration}.
     */
    protected Durability getDefaultDurability() {
        return myClient.getDefaultDurability();
    }
}