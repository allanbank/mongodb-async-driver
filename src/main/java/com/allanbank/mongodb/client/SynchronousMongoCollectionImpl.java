/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.List;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.ListenableFuture;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.util.FutureUtils;

/**
 * Implementation of the {@link MongoCollection} interface including the
 * synchronous methods.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SynchronousMongoCollectionImpl extends
        AbstractAsyncMongoCollection implements MongoCollection {

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
    public SynchronousMongoCollectionImpl(final Client client,
            final MongoDatabase database, final String name) {
        super(client, database, name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregateAsync(Aggregate)}.
     * </p>
     * 
     * @see #aggregateAsync(Aggregate)
     */
    @Override
    public MongoIterator<Document> aggregate(final Aggregate command)
            throws MongoDbException {
        return FutureUtils.unwrap(aggregateAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregate(Aggregate)}.
     * </p>
     */
    @Override
    public MongoIterator<Document> aggregate(final Aggregate.Builder command)
            throws MongoDbException {
        return aggregate(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #count(DocumentAssignable,ReadPreference)}
     * method with {@link #getReadPreference()} as the <tt>readPreference</tt>
     * argument and an empty {@code query} document.
     * </p>
     */
    @Override
    public long count() throws MongoDbException {
        return count(BuilderFactory.start(), getReadPreference());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Count)} and unwrap the result.
     * </p>
     */
    @Override
    public long count(final Count count) throws MongoDbException {
        final ListenableFuture<Long> future = countAsync(count);

        return FutureUtils.unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #count(Count) count(count.build())}.
     * </p>
     */
    @Override
    public long count(final Count.Builder count) throws MongoDbException {
        return count(count.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #count(DocumentAssignable, ReadPreference)}
     * method with {@link #getReadPreference()} as the <tt>readPreference</tt>
     * argument.
     * </p>
     */
    @Override
    public long count(final DocumentAssignable query) throws MongoDbException {
        return count(query, getReadPreference());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(DocumentAssignable, ReadPreference)} method.
     * </p>
     */
    @Override
    public long count(final DocumentAssignable query,
            final ReadPreference readPreference) throws MongoDbException {

        final ListenableFuture<Long> future = countAsync(query, readPreference);

        return FutureUtils.unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #count(DocumentAssignable,ReadPreference)}
     * method with an empty {@code query} document.
     * </p>
     */
    @Override
    public long count(final ReadPreference readPreference)
            throws MongoDbException {
        return count(BuilderFactory.start(), readPreference);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #createIndex(String, boolean, Element...)}
     * method with <code>null</code> for the name.
     * </p>
     * 
     * @see #createIndex(String, boolean, Element...)
     */
    @Override
    public void createIndex(final boolean unique, final Element... keys)
            throws MongoDbException {
        createIndex(null, unique, keys);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #createIndex(String,DocumentAssignable,Element...)} method with
     * <code>null</code> for <tt>name</tt>.
     * </p>
     * 
     * @see #createIndex(String,DocumentAssignable,Element...)
     */
    @Override
    public void createIndex(final DocumentAssignable options,
            final Element... keys) throws MongoDbException {
        createIndex(null, options, keys);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #createIndex(DocumentAssignable, Element...)} method with
     * {@link #EMPTY_INDEX_OPTIONS} for <tt>options</tt>.
     * </p>
     * 
     * @see #createIndex(DocumentAssignable, Element...)
     */
    @Override
    public void createIndex(final Element... keys) throws MongoDbException {
        createIndex(EMPTY_INDEX_OPTIONS, keys);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #createIndex(String,DocumentAssignable,Element...)} method with
     * {@link #UNIQUE_INDEX_OPTIONS} if {@code unique} is <code>true</code> or
     * {@link #EMPTY_INDEX_OPTIONS} id {@code unique} is <code>false</code>.
     * </p>
     * 
     * @see #createIndex(String, DocumentAssignable, Element...)
     */
    @Override
    public void createIndex(final String name, final boolean unique,
            final Element... keys) throws MongoDbException {
        createIndex(name, unique ? UNIQUE_INDEX_OPTIONS : EMPTY_INDEX_OPTIONS,
                keys);
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

        final SynchronousMongoCollectionImpl indexCollection = new SynchronousMongoCollectionImpl(
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
     * Overridden to call the
     * {@link #delete(DocumentAssignable, boolean, Durability)} method with
     * false as the <tt>singleDelete</tt> argument and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #delete(DocumentAssignable, boolean, Durability)
     */
    @Override
    public long delete(final DocumentAssignable query) throws MongoDbException {
        return delete(query, DELETE_SINGLE_DELETE_DEFAULT, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #delete(DocumentAssignable, boolean, Durability)} method with the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #delete(DocumentAssignable, boolean, Durability)
     */
    @Override
    public long delete(final DocumentAssignable query,
            final boolean singleDelete) throws MongoDbException {
        return delete(query, singleDelete, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(DocumentAssignable, boolean, Durability)} method.
     * </p>
     * 
     * @see #deleteAsync(DocumentAssignable, boolean, Durability)
     */
    @Override
    public long delete(final DocumentAssignable query,
            final boolean singleDelete, final Durability durability)
            throws MongoDbException {

        final ListenableFuture<Long> future = deleteAsync(query, singleDelete,
                durability);

        return FutureUtils.unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(DocumentAssignable, boolean, Durability)} method with
     * false as the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see #delete(DocumentAssignable, boolean, Durability)
     */
    @Override
    public long delete(final DocumentAssignable query,
            final Durability durability) throws MongoDbException {
        return delete(query, DELETE_SINGLE_DELETE_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Distinct)}.
     * </p>
     */
    @Override
    public MongoIterator<Element> distinct(final Distinct command)
            throws MongoDbException {
        return FutureUtils.unwrap(distinctAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinct(Distinct)}.
     * </p>
     */
    @Override
    public MongoIterator<Element> distinct(final Distinct.Builder command)
            throws MongoDbException {
        return distinct(command.build());
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
     * To generate the name of the index and then drop it.
     * </p>
     */
    @Override
    public boolean dropIndex(final IntegerElement... keys)
            throws MongoDbException {
        return dropIndex(buildIndexName(keys));
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
     */
    @Override
    public boolean exists() throws MongoDbException {
        return myDatabase.listCollectionNames().contains(getName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Aggregate)} method.
     * </p>
     */
    @Override
    public Document explain(final Aggregate aggregation)
            throws MongoDbException {
        return FutureUtils.unwrap(explainAsync(aggregation));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Aggregate)} method.
     * </p>
     */
    @Override
    public Document explain(final Aggregate.Builder aggregation)
            throws MongoDbException {
        return FutureUtils.unwrap(explainAsync(aggregation.build()));

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explain(Find)} method.
     * </p>
     * 
     * @see #explain(Find)
     */
    @Override
    public Document explain(final DocumentAssignable query)
            throws MongoDbException {
        return explain(new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Find)} method.
     * </p>
     * 
     * @see #explainAsync(Find)
     */
    @Override
    public Document explain(final Find query) throws MongoDbException {
        return FutureUtils.unwrap(explainAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explain(Find)} method.
     * </p>
     */
    @Override
    public Document explain(final Find.Builder query) throws MongoDbException {
        return explain(query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(DocumentAssignable)} method.
     * </p>
     * 
     * @see #findAsync(DocumentAssignable)
     */
    @Override
    public MongoIterator<Document> find(final DocumentAssignable query)
            throws MongoDbException {
        return FutureUtils.unwrap(findAsync(query));
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
    public MongoIterator<Document> find(final Find query)
            throws MongoDbException {
        return FutureUtils.unwrap(findAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #find(Find)} method.
     * </p>
     */
    @Override
    public MongoIterator<Document> find(final Find.Builder query)
            throws MongoDbException {
        return find(query.build());
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
        return FutureUtils.unwrap(findAndModifyAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAndModify(FindAndModify)}.
     * </p>
     */
    @Override
    public Document findAndModify(final FindAndModify.Builder command)
            throws MongoDbException {
        return findAndModify(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(DocumentAssignable)}.
     * </p>
     * 
     * @see #findOneAsync(DocumentAssignable)
     */
    @Override
    public Document findOne(final DocumentAssignable query)
            throws MongoDbException {
        return FutureUtils.unwrap(findOneAsync(query));
    }

    /**
     * <p>
     * Overridden to call the
     * {@link #findOneAsync(com.allanbank.mongodb.Callback, Find)}.
     * </p>
     * 
     * @see #findOneAsync(com.allanbank.mongodb.Callback, Find)
     */
    @Override
    public Document findOne(final Find query) throws MongoDbException {
        return FutureUtils.unwrap(findOneAsync(query));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOne(Find)} method.
     * </p>
     */
    @Override
    public Document findOne(final Find.Builder query) throws MongoDbException {
        return findOne(query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(GroupBy)}.
     * </p>
     */
    @Override
    public MongoIterator<Element> groupBy(final GroupBy command)
            throws MongoDbException {
        return FutureUtils.unwrap(groupByAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupBy(GroupBy)}.
     * </p>
     */
    @Override
    public MongoIterator<Element> groupBy(final GroupBy.Builder command)
            throws MongoDbException {
        return groupBy(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insert(boolean, Durability, DocumentAssignable...)} method with
     * the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #insert(boolean, Durability, DocumentAssignable[])
     */
    @Override
    public int insert(final boolean continueOnError,
            final DocumentAssignable... documents) throws MongoDbException {
        return insert(continueOnError, getDurability(), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(boolean, Durability, DocumentAssignable...)} method.
     * </p>
     * 
     * @see #insertAsync(boolean, Durability, DocumentAssignable[])
     */
    @Override
    public int insert(final boolean continueOnError,
            final Durability durability, final DocumentAssignable... documents)
            throws MongoDbException {
        final ListenableFuture<Integer> future = insertAsync(continueOnError,
                durability, documents);

        return FutureUtils.unwrap(future).intValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insert(boolean, Durability, DocumentAssignable...)} method with
     * <tt>continueOnError</tt> set to false and the {@link #getDurability()
     * default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public int insert(final DocumentAssignable... documents)
            throws MongoDbException {
        return insert(INSERT_CONTINUE_ON_ERROR_DEFAULT, getDurability(),
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insert(boolean, Durability, DocumentAssignable...)} method with
     * <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see #insert(boolean, Durability, DocumentAssignable[])
     */
    @Override
    public int insert(final Durability durability,
            final DocumentAssignable... documents) throws MongoDbException {
        return insert(INSERT_CONTINUE_ON_ERROR_DEFAULT, durability, documents);
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
     * Overridden to call the {@link #mapReduceAsync(MapReduce)}.
     * </p>
     * 
     * @see #mapReduceAsync(MapReduce)
     */
    @Override
    public MongoIterator<Document> mapReduce(final MapReduce command)
            throws MongoDbException {
        return FutureUtils.unwrap(mapReduceAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduce(MapReduce)}.
     * </p>
     */
    @Override
    public MongoIterator<Document> mapReduce(final MapReduce.Builder command)
            throws MongoDbException {
        return mapReduce(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #save(DocumentAssignable, Durability)}
     * using the {@link #getDurability() default durability}.
     * </p>
     */
    @Override
    public int save(final DocumentAssignable document) throws MongoDbException {
        return save(document, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #saveAsync(DocumentAssignable, Durability)}
     * .
     * </p>
     */
    @Override
    public int save(final DocumentAssignable document,
            final Durability durability) throws MongoDbException {
        return FutureUtils.unwrap(saveAsync(document, durability)).intValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a {@link BatchedAsyncMongoCollection}.
     * </p>
     */
    @Override
    public BatchedAsyncMongoCollection startBatch() {
        return new BatchedAsyncMongoCollectionImpl(myClient, myDatabase, myName);
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
     * Overridden to call the
     * {@link #textSearchAsync(com.allanbank.mongodb.builder.Text)}.
     * </p>
     * 
     * @see #textSearchAsync(com.allanbank.mongodb.builder.Text)
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This method will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    @Override
    public MongoIterator<com.allanbank.mongodb.builder.TextResult> textSearch(
            final com.allanbank.mongodb.builder.Text command)
            throws MongoDbException {
        return FutureUtils.unwrap(textSearchAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #textSearch(com.allanbank.mongodb.builder.Text)}.
     * </p>
     * 
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This method will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    @Override
    public MongoIterator<com.allanbank.mongodb.builder.TextResult> textSearch(
            final com.allanbank.mongodb.builder.Text.Builder command)
            throws MongoDbException {
        return textSearch(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #update(DocumentAssignable, DocumentAssignable, boolean, boolean,
     *      Durability)
     */
    @Override
    public long update(final DocumentAssignable query,
            final DocumentAssignable update) throws MongoDbException {
        return update(query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method with the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #update(DocumentAssignable, DocumentAssignable, boolean, boolean,
     *      Durability)
     */
    @Override
    public long update(final DocumentAssignable query,
            final DocumentAssignable update, final boolean multiUpdate,
            final boolean upsert) throws MongoDbException {
        return update(query, update, multiUpdate, upsert, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method.
     * </p>
     * 
     * @see #updateAsync(DocumentAssignable, DocumentAssignable, boolean,
     *      boolean, Durability)
     */
    @Override
    public long update(final DocumentAssignable query,
            final DocumentAssignable update, final boolean multiUpdate,
            final boolean upsert, final Durability durability)
            throws MongoDbException {

        final ListenableFuture<Long> future = updateAsync(query, update,
                multiUpdate, upsert, durability);

        return FutureUtils.unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #update(DocumentAssignable, DocumentAssignable, boolean, boolean,
     *      Durability)
     */
    @Override
    public long update(final DocumentAssignable query,
            final DocumentAssignable update, final Durability durability)
            throws MongoDbException {
        return update(query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);
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
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #writeAsync(BatchedWrite)}.
     * </p>
     * 
     * @see #writeAsync(BatchedWrite)
     */
    @Override
    public long write(final BatchedWrite write) throws MongoDbException {
        final ListenableFuture<Long> future = writeAsync(write);

        return FutureUtils.unwrap(future).longValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #write(BatchedWrite)}.
     * </p>
     * 
     * @see #write(BatchedWrite)
     */
    @Override
    public long write(final BatchedWrite.Builder write) throws MongoDbException {
        return write(write.build());
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
     * Generates a name for the index based on the keys.
     * 
     * @param keys
     *            The keys for the index.
     * @return The name for the index.
     */
    protected String buildIndexName(final Element... keys) {
        final StringBuilder nameBuilder = new StringBuilder();
        for (final Element key : keys) {
            if (nameBuilder.length() > 0) {
                nameBuilder.append('_');
            }
            nameBuilder.append(key.getName().replace(' ', '_'));
            nameBuilder.append("_");
            if (key instanceof NumericElement) {
                nameBuilder.append(((NumericElement) key).getIntValue());
            }
            else {
                nameBuilder.append(key.getValueAsString());
            }
        }
        return nameBuilder.toString();
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
}
