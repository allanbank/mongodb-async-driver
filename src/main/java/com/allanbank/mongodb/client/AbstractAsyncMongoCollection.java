/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.AsyncMongoCollection;
import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.ListenableFuture;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.Text;
import com.allanbank.mongodb.builder.TextResult;

/**
 * Helper class for forward all methods to the canonical version (which is
 * abstract in this class).
 * <p>
 * This class keeps the clutter in the derived class to a minimum and also deals
 * with the conversion of the asynchronous method invocations into synchronous
 * methods for those uses cases that do not require an asynchronous interface.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractAsyncMongoCollection extends
        AbstractMongoOperations implements AsyncMongoCollection {

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
    public AbstractAsyncMongoCollection(final Client client,
            final MongoDatabase database, final String name) {
        super(client, database, name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregateAsync(Callback, Aggregate)}.
     * </p>
     * 
     * @see #aggregateAsync(Callback, Aggregate)
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> aggregateAsync(
            final Aggregate command) throws MongoDbException {
        final FutureCallback<MongoIterator<Document>> future;

        future = new FutureCallback<MongoIterator<Document>>(getLockType());

        aggregateAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregateAsync(Aggregate)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> aggregateAsync(
            final Aggregate.Builder command) throws MongoDbException {
        return aggregateAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregateAsync(Callback, Aggregate)}.
     * </p>
     */
    @Override
    public void aggregateAsync(final Callback<MongoIterator<Document>> results,
            final Aggregate.Builder command) throws MongoDbException {
        aggregateAsync(results, command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #aggregateAsync(Callback, Aggregate)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void aggregateAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final Aggregate command) throws MongoDbException {
        aggregateAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(
                results), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #aggregateAsync(Callback, Aggregate.Builder)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void aggregateAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final Aggregate.Builder command) throws MongoDbException {
        aggregateAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(
                results), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(DocumentAssignable,ReadPreference)} method with
     * {@link #getReadPreference()} as the <tt>readPreference</tt> argument and
     * an empty {@code query} document.
     * </p>
     */
    @Override
    public ListenableFuture<Long> countAsync() throws MongoDbException {
        return countAsync(BuilderFactory.start(), getReadPreference());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback,DocumentAssignable,ReadPreference)} method
     * with {@link #getReadPreference()} as the <tt>readPreference</tt> argument
     * and an empty {@code query} document.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results)
            throws MongoDbException {
        countAsync(results, BuilderFactory.start(), getReadPreference());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Count)
     * countAsync(results, count.build())}.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results,
            final Count.Builder count) throws MongoDbException {
        countAsync(results, count.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback, DocumentAssignable, ReadPreference)} method
     * with {@link #getReadPreference()} as the <tt>readPreference</tt>
     * argument.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results,
            final DocumentAssignable query) throws MongoDbException {
        countAsync(results, query, getReadPreference());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Count)} method with
     * the query and read preferences set.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results,
            final DocumentAssignable query, final ReadPreference readPreference)
            throws MongoDbException {
        countAsync(results,
                Count.builder().query(query).readPreference(readPreference)
                        .build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback,DocumentAssignable,ReadPreference)} method
     * with an empty {@code query} document.
     * </p>
     */
    @Override
    public void countAsync(final Callback<Long> results,
            final ReadPreference readPreference) throws MongoDbException {
        countAsync(results, BuilderFactory.start(), readPreference);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Count)}.
     * </p>
     * On an error counting the documents.
     */
    @Override
    public ListenableFuture<Long> countAsync(final Count count)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        countAsync(future, count);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Count)
     * countAsync(count.build())}.
     * </p>
     */
    @Override
    public ListenableFuture<Long> countAsync(final Count.Builder count)
            throws MongoDbException {
        return countAsync(count.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback, DocumentAssignable, ReadPreference)} method
     * with {@link #getReadPreference()} as the <tt>readPreference</tt>
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
    public ListenableFuture<Long> countAsync(final DocumentAssignable query)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        countAsync(future, query, getReadPreference());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback, DocumentAssignable, ReadPreference)} method.
     * </p>
     */
    @Override
    public ListenableFuture<Long> countAsync(final DocumentAssignable query,
            final ReadPreference readPreference) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        countAsync(future, query, readPreference);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results)
            throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Count)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results, final Count count)
            throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results), count);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, Count.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results,
            final Count.Builder count) throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results), count);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query) throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results), query);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(Callback, DocumentAssignable, ReadPreference)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final ReadPreference readPreference)
            throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results), query,
                readPreference);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #countAsync(Callback, ReadPreference)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void countAsync(final LambdaCallback<Long> results,
            final ReadPreference readPreference) throws MongoDbException {
        countAsync(new LambdaCallbackAdapter<Long>(results), readPreference);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #countAsync(DocumentAssignable,ReadPreference)} method with an
     * empty {@code query} document.
     * </p>
     */
    @Override
    public ListenableFuture<Long> countAsync(final ReadPreference readPreference)
            throws MongoDbException {
        return countAsync(BuilderFactory.start(), readPreference);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with false as the <tt>singleDelete</tt> argument and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #deleteAsync(Callback, DocumentAssignable, boolean, Durability)
     */
    @Override
    public void deleteAsync(final Callback<Long> results,
            final DocumentAssignable query) throws MongoDbException {
        deleteAsync(results, query, DELETE_SINGLE_DELETE_DEFAULT,
                getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable)
     */
    @Override
    public void deleteAsync(final Callback<Long> results,
            final DocumentAssignable query, final boolean singleDelete)
            throws MongoDbException {
        deleteAsync(results, query, singleDelete, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with false as the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable, boolean)
     */
    @Override
    public void deleteAsync(final Callback<Long> results,
            final DocumentAssignable query, final Durability durability)
            throws MongoDbException {
        deleteAsync(results, query, DELETE_SINGLE_DELETE_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with false as the <tt>singleDelete</tt> argument and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Long> deleteAsync(final DocumentAssignable query)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        deleteAsync(future, query, DELETE_SINGLE_DELETE_DEFAULT,
                getDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Long> deleteAsync(final DocumentAssignable query,
            final boolean singleDelete) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        deleteAsync(future, query, singleDelete, getDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #deleteAsync(Callback, DocumentAssignable)}
     * method.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Long> deleteAsync(final DocumentAssignable query,
            final boolean singleDelete, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        deleteAsync(future, query, singleDelete, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #deleteAsync(Callback, DocumentAssignable)}
     * method with false as the <tt>singleDelete</tt> argument.
     * </p>
     * 
     * @see MongoCollection#deleteAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Long> deleteAsync(final DocumentAssignable query,
            final Durability durability) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        deleteAsync(future, query, DELETE_SINGLE_DELETE_DEFAULT, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #deleteAsync(Callback, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void deleteAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query) throws MongoDbException {
        deleteAsync(new LambdaCallbackAdapter<Long>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void deleteAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final boolean singleDelete)
            throws MongoDbException {
        deleteAsync(new LambdaCallbackAdapter<Long>(results), query,
                singleDelete);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void deleteAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final boolean singleDelete,
            final Durability durability) throws MongoDbException {
        deleteAsync(new LambdaCallbackAdapter<Long>(results), query,
                singleDelete, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #deleteAsync(Callback, DocumentAssignable, Durability)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void deleteAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final Durability durability)
            throws MongoDbException {
        deleteAsync(new LambdaCallbackAdapter<Long>(results), query, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Callback, Distinct)}.
     * </p>
     */
    @Override
    public void distinctAsync(final Callback<MongoIterator<Element>> results,
            final Distinct.Builder command) throws MongoDbException {
        distinctAsync(results, command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Callback, Distinct)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Element>> distinctAsync(
            final Distinct command) throws MongoDbException {
        final FutureCallback<MongoIterator<Element>> future = new FutureCallback<MongoIterator<Element>>(
                getLockType());

        distinctAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Distinct)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Element>> distinctAsync(
            final Distinct.Builder command) throws MongoDbException {
        return distinctAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Callback, Distinct)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void distinctAsync(
            final LambdaCallback<MongoIterator<Element>> results,
            final Distinct command) throws MongoDbException {
        distinctAsync(
                new LambdaCallbackAdapter<MongoIterator<Element>>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #distinctAsync(Callback, Distinct.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void distinctAsync(
            final LambdaCallback<MongoIterator<Element>> results,
            final Distinct.Builder command) throws MongoDbException {
        distinctAsync(
                new LambdaCallbackAdapter<MongoIterator<Element>>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback,Aggregate)} method.
     * </p>
     */
    @Override
    public ListenableFuture<Document> explainAsync(final Aggregate aggregation)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        explainAsync(future, aggregation);

        return future;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback,Aggregate)} method.
     * </p>
     */
    @Override
    public ListenableFuture<Document> explainAsync(
            final Aggregate.Builder aggregation) throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        explainAsync(future, aggregation.build());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback,Aggregate)} method.
     * </p>
     */
    @Override
    public void explainAsync(final Callback<Document> results,
            final Aggregate.Builder aggregation) throws MongoDbException {
        explainAsync(results, aggregation.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback,Find)} method.
     * </p>
     */
    @Override
    public void explainAsync(final Callback<Document> results,
            final Find.Builder query) throws MongoDbException {
        explainAsync(results, query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback,Find)} method.
     * </p>
     * 
     * @see #explainAsync(Callback,Find)
     */
    @Override
    public ListenableFuture<Document> explainAsync(final Find query)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        explainAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Find)} method.
     * </p>
     */
    @Override
    public ListenableFuture<Document> explainAsync(final Find.Builder query)
            throws MongoDbException {
        return explainAsync(query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback, Aggregate)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void explainAsync(final LambdaCallback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        explainAsync(new LambdaCallbackAdapter<Document>(results), aggregation);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback, Aggregate.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void explainAsync(final LambdaCallback<Document> results,
            final Aggregate.Builder aggregation) throws MongoDbException {
        explainAsync(new LambdaCallbackAdapter<Document>(results), aggregation);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback, Find)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void explainAsync(final LambdaCallback<Document> results,
            final Find query) throws MongoDbException {
        explainAsync(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #explainAsync(Callback, Find.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void explainAsync(final LambdaCallback<Document> results,
            final Find.Builder query) throws MongoDbException {
        explainAsync(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findAndModifyAsync(Callback,FindAndModify)}.
     * </p>
     */
    @Override
    public void findAndModifyAsync(final Callback<Document> results,
            final FindAndModify.Builder command) throws MongoDbException {
        findAndModifyAsync(results, command.build());
    }

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
    public ListenableFuture<Document> findAndModifyAsync(
            final FindAndModify command) throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        findAndModifyAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAndModifyAsync(FindAndModify)}.
     * </p>
     */
    @Override
    public ListenableFuture<Document> findAndModifyAsync(
            final FindAndModify.Builder command) throws MongoDbException {
        return findAndModifyAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findAndModifyAsync(Callback, FindAndModify)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findAndModifyAsync(final LambdaCallback<Document> results,
            final FindAndModify command) throws MongoDbException {
        findAndModifyAsync(new LambdaCallbackAdapter<Document>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findAndModifyAsync(Callback, FindAndModify.Builder)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findAndModifyAsync(final LambdaCallback<Document> results,
            final FindAndModify.Builder command) throws MongoDbException {
        findAndModifyAsync(new LambdaCallbackAdapter<Document>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find)}.
     * </p>
     * 
     * @see #findAsync(Callback, DocumentAssignable)
     */
    @Override
    public void findAsync(final Callback<MongoIterator<Document>> results,
            final DocumentAssignable query) throws MongoDbException {
        findAsync(results, new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback,Find)} method.
     * </p>
     */
    @Override
    public void findAsync(final Callback<MongoIterator<Document>> results,
            final Find.Builder query) throws MongoDbException {
        findAsync(results, query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, DocumentAssignable)}.
     * </p>
     * 
     * @see #findAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> findAsync(
            final DocumentAssignable query) throws MongoDbException {
        final FutureCallback<MongoIterator<Document>> future = new FutureCallback<MongoIterator<Document>>(
                getLockType());

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
    public ListenableFuture<MongoIterator<Document>> findAsync(final Find query)
            throws MongoDbException {
        final FutureCallback<MongoIterator<Document>> future = new FutureCallback<MongoIterator<Document>>(
                getLockType());

        findAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Find)} method.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> findAsync(
            final Find.Builder query) throws MongoDbException {
        return findAsync(query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final DocumentAssignable query) throws MongoDbException {
        findAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(results),
                query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final Find query) throws MongoDbException {
        findAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(results),
                query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find.Builder)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final Find.Builder query) throws MongoDbException {
        findAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(results),
                query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Callback, Find)}.
     * </p>
     * 
     * @see #findOneAsync(Callback, Find)
     */
    @Override
    public void findOneAsync(final Callback<Document> results,
            final DocumentAssignable query) throws MongoDbException {
        findOneAsync(results, new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Callback, Find)} method.
     * </p>
     */
    @Override
    public void findOneAsync(final Callback<Document> results,
            final Find.Builder query) throws MongoDbException {
        findOneAsync(results, query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #findOneAsync(Callback, DocumentAssignable)}.
     * </p>
     * 
     * @see #findOneAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> findOneAsync(
            final DocumentAssignable query) throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        findOneAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Callback, Find)}.
     * </p>
     * 
     * @see #findOneAsync(Callback, Find)
     */
    @Override
    public ListenableFuture<Document> findOneAsync(final Find query)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        findOneAsync(future, query);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findOneAsync(Find)} method.
     * </p>
     */
    @Override
    public ListenableFuture<Document> findOneAsync(final Find.Builder query)
            throws MongoDbException {
        return findOneAsync(query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findOneAsync(final LambdaCallback<Document> results,
            final DocumentAssignable query) throws MongoDbException {
        findOneAsync(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findOneAsync(final LambdaCallback<Document> results,
            final Find query) throws MongoDbException {
        findOneAsync(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #findAsync(Callback, Find.Builder)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void findOneAsync(final LambdaCallback<Document> results,
            final Find.Builder query) throws MongoDbException {
        findOneAsync(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(Callback,GroupBy)}.
     * </p>
     */
    @Override
    public void groupByAsync(final Callback<MongoIterator<Element>> results,
            final GroupBy.Builder command) throws MongoDbException {
        groupByAsync(results, command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(Callback, GroupBy)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Element>> groupByAsync(
            final GroupBy command) throws MongoDbException {
        final FutureCallback<MongoIterator<Element>> future = new FutureCallback<MongoIterator<Element>>(
                getLockType());

        groupByAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(GroupBy)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Element>> groupByAsync(
            final GroupBy.Builder command) throws MongoDbException {
        return groupByAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(Callback, GroupBy)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void groupByAsync(
            final LambdaCallback<MongoIterator<Element>> results,
            final GroupBy command) throws MongoDbException {
        groupByAsync(
                new LambdaCallbackAdapter<MongoIterator<Element>>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #groupByAsync(Callback, GroupBy.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void groupByAsync(
            final LambdaCallback<MongoIterator<Element>> results,
            final GroupBy.Builder command) throws MongoDbException {
        groupByAsync(
                new LambdaCallbackAdapter<MongoIterator<Element>>(results),
                command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public ListenableFuture<Integer> insertAsync(final boolean continueOnError,
            final DocumentAssignable... documents) throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        insertAsync(future, continueOnError, getDurability(), documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with <tt>continueOnError</tt> set to false and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public ListenableFuture<Integer> insertAsync(final boolean continueOnError,
            final Durability durability, final DocumentAssignable... documents)
            throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        insertAsync(future, continueOnError, durability, documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final boolean continueOnError,
            final DocumentAssignable... documents) throws MongoDbException {
        insertAsync(results, continueOnError, getDurability(), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with <tt>continueOnError</tt> set to false and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final DocumentAssignable... documents) throws MongoDbException {
        insertAsync(results, INSERT_CONTINUE_ON_ERROR_DEFAULT, getDurability(),
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public void insertAsync(final Callback<Integer> results,
            final Durability durability, final DocumentAssignable... documents)
            throws MongoDbException {
        insertAsync(results, INSERT_CONTINUE_ON_ERROR_DEFAULT, durability,
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with <tt>continueOnError</tt> set to false and the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public ListenableFuture<Integer> insertAsync(
            final DocumentAssignable... documents) throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        insertAsync(future, INSERT_CONTINUE_ON_ERROR_DEFAULT, getDurability(),
                documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * method with <tt>continueOnError</tt> set to false.
     * </p>
     * 
     * @see MongoCollection#insertAsync(Callback, boolean, Durability,
     *      DocumentAssignable[])
     */
    @Override
    public ListenableFuture<Integer> insertAsync(final Durability durability,
            final DocumentAssignable... documents) throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        insertAsync(future, INSERT_CONTINUE_ON_ERROR_DEFAULT, durability,
                documents);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, DocumentAssignable[])} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void insertAsync(final LambdaCallback<Integer> results,
            final boolean continueOnError,
            final DocumentAssignable... documents) throws MongoDbException {
        insertAsync(new LambdaCallbackAdapter<Integer>(results),
                continueOnError, documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, boolean, Durability, DocumentAssignable[])}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void insertAsync(final LambdaCallback<Integer> results,
            final boolean continueOnError, final Durability durability,
            final DocumentAssignable... documents) throws MongoDbException {
        insertAsync(new LambdaCallbackAdapter<Integer>(results),
                continueOnError, durability, documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, DocumentAssignable[])} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void insertAsync(final LambdaCallback<Integer> results,
            final DocumentAssignable... documents) throws MongoDbException {
        insertAsync(new LambdaCallbackAdapter<Integer>(results), documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #insertAsync(Callback, Durability, DocumentAssignable[])} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void insertAsync(final LambdaCallback<Integer> results,
            final Durability durability, final DocumentAssignable... documents)
            throws MongoDbException {
        insertAsync(new LambdaCallbackAdapter<Integer>(results), durability,
                documents);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduceAsync(Callback,MapReduce)}.
     * </p>
     */
    @Override
    public void mapReduceAsync(final Callback<MongoIterator<Document>> results,
            final MapReduce.Builder command) throws MongoDbException {
        mapReduceAsync(results, command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduceAsync(Callback, MapReduce)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void mapReduceAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final MapReduce command) throws MongoDbException {
        mapReduceAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(
                results), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #mapReduceAsync(Callback, MapReduce.Builder)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void mapReduceAsync(
            final LambdaCallback<MongoIterator<Document>> results,
            final MapReduce.Builder command) throws MongoDbException {
        mapReduceAsync(new LambdaCallbackAdapter<MongoIterator<Document>>(
                results), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduceAsync(Callback, MapReduce)}.
     * </p>
     * 
     * @see #mapReduceAsync(Callback, MapReduce)
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> mapReduceAsync(
            final MapReduce command) throws MongoDbException {
        final FutureCallback<MongoIterator<Document>> future = new FutureCallback<MongoIterator<Document>>(
                getLockType());

        mapReduceAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #mapReduceAsync(MapReduce)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> mapReduceAsync(
            final MapReduce.Builder command) throws MongoDbException {
        return mapReduceAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #saveAsync(Callback, DocumentAssignable, Durability)} using the
     * {@link #getDurability() default durability}.
     * </p>
     */
    @Override
    public void saveAsync(final Callback<Integer> results,
            final DocumentAssignable document) throws MongoDbException {
        saveAsync(results, document, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #saveAsync(Callback, DocumentAssignable, Durability)} using the
     * {@link #getDurability() default durability}.
     * </p>
     */
    @Override
    public ListenableFuture<Integer> saveAsync(final DocumentAssignable document)
            throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        saveAsync(future, document, getDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #saveAsync(Callback, DocumentAssignable, Durability)}.
     * </p>
     */
    @Override
    public ListenableFuture<Integer> saveAsync(
            final DocumentAssignable document, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Integer> future = new FutureCallback<Integer>(
                getLockType());

        saveAsync(future, document, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #saveAsync(Callback, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void saveAsync(final LambdaCallback<Integer> results,
            final DocumentAssignable document) throws MongoDbException {
        saveAsync(new LambdaCallbackAdapter<Integer>(results), document);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #saveAsync(Callback, DocumentAssignable, Durability)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void saveAsync(final LambdaCallback<Integer> results,
            final DocumentAssignable document, final Durability durability)
            throws MongoDbException {
        saveAsync(new LambdaCallbackAdapter<Integer>(results), document,
                durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Aggregate)} method
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final LambdaCallback<Document> results,
            final Aggregate aggregation) throws MongoDbException {
        return stream(new LambdaCallbackAdapter<Document>(results), aggregation);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Aggregate.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final LambdaCallback<Document> results,
            final Aggregate.Builder aggregation) throws MongoDbException {
        return stream(new LambdaCallbackAdapter<Document>(results), aggregation);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Find)} method with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final LambdaCallback<Document> results,
            final Find query) throws MongoDbException {
        return stream(new LambdaCallbackAdapter<Document>(results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Find.Builder)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final LambdaCallback<Document> results,
            final Find.Builder aggregation) throws MongoDbException {
        return stream(new LambdaCallbackAdapter<Document>(results), aggregation);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Aggregate)}.
     * </p>
     * 
     * @see #stream(StreamCallback, Aggregate)
     */
    @Override
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Aggregate.Builder aggregation) throws MongoDbException {
        return stream(results, aggregation.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Find)} method.
     * </p>
     */
    @Override
    public MongoCursorControl stream(final StreamCallback<Document> results,
            final Find.Builder query) throws MongoDbException {
        return streamingFind(results, query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #streamingFind(Callback, Find)}.
     * </p>
     * 
     * @see #streamingFind(Callback, Find)
     */
    @Deprecated
    @Override
    public MongoCursorControl streamingFind(final Callback<Document> results,
            final DocumentAssignable query) throws MongoDbException {
        return streamingFind(results, new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Find)}.
     * </p>
     * 
     * @see #stream(StreamCallback, Find)
     */
    @Deprecated
    @Override
    public MongoCursorControl streamingFind(final Callback<Document> results,
            final Find query) throws MongoDbException {
        return stream(
                new com.allanbank.mongodb.client.callback.LegacyStreamCallbackAdapter(
                        results), query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #streamingFind(StreamCallback, DocumentAssignable)} method with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public MongoCursorControl streamingFind(
            final LambdaCallback<Document> results,
            final DocumentAssignable query) throws MongoDbException {
        return streamingFind(new LambdaCallbackAdapter<Document>(results),
                query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #stream(StreamCallback, Find)}.
     * </p>
     * 
     * @see #stream(StreamCallback, Find)
     */
    @Override
    public MongoCursorControl streamingFind(
            final StreamCallback<Document> results,
            final DocumentAssignable query) throws MongoDbException {
        return stream(results, new Find.Builder(query).build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #streamingFind(StreamCallback, Find)}.
     * </p>
     * 
     * @see #stream(StreamCallback, Find)
     */
    @Deprecated
    @Override
    public MongoCursorControl streamingFind(
            final StreamCallback<Document> results, final Find query)
            throws MongoDbException {
        return stream(results, query);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #streamingFind(StreamCallback, Find)}
     * method.
     * </p>
     */
    @Deprecated
    @Override
    public MongoCursorControl streamingFind(
            final StreamCallback<Document> results, final Find.Builder query)
            throws MongoDbException {
        return streamingFind(results, query.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #textSearchAsync(Callback, Text)}.
     * </p>
     */
    @Override
    public void textSearchAsync(
            final Callback<MongoIterator<TextResult>> results,
            final Text.Builder command) throws MongoDbException {
        textSearchAsync(results, command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #textSearchAsync(Callback, Text)}.
     * </p>
     * 
     * @see #textSearchAsync(Callback, Text)
     */
    @Override
    public ListenableFuture<MongoIterator<TextResult>> textSearchAsync(
            final Text command) throws MongoDbException {
        final FutureCallback<MongoIterator<TextResult>> future = new FutureCallback<MongoIterator<TextResult>>(
                getLockType());

        textSearchAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #textSearchAsync(Text)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<TextResult>> textSearchAsync(
            final Text.Builder command) throws MongoDbException {
        return textSearchAsync(command.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update)
            throws MongoDbException {
        updateAsync(results, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * using the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final boolean multiUpdate, final boolean upsert)
            throws MongoDbException {
        updateAsync(results, query, update, multiUpdate, upsert,
                getDurability());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public void updateAsync(final Callback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final Durability durability) throws MongoDbException {
        updateAsync(results, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * with multiUpdate set to true, upsert set to false, and using the
     * {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public ListenableFuture<Long> updateAsync(final DocumentAssignable query,
            final DocumentAssignable update) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        updateAsync(future, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, getDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * using the {@link #getDurability() default durability}.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public ListenableFuture<Long> updateAsync(final DocumentAssignable query,
            final DocumentAssignable update, final boolean multiUpdate,
            final boolean upsert) throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        updateAsync(future, query, update, multiUpdate, upsert, getDurability());

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public ListenableFuture<Long> updateAsync(final DocumentAssignable query,
            final DocumentAssignable update, final boolean multiUpdate,
            final boolean upsert, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        updateAsync(future, query, update, multiUpdate, upsert, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * with multiUpdate set to true, and upsert set to false.
     * </p>
     * 
     * @see #updateAsync(Callback, DocumentAssignable, DocumentAssignable,
     *      boolean, boolean, Durability)
     */
    @Override
    public ListenableFuture<Long> updateAsync(final DocumentAssignable query,
            final DocumentAssignable update, final Durability durability)
            throws MongoDbException {
        final FutureCallback<Long> future = new FutureCallback<Long>(
                getLockType());

        updateAsync(future, query, update, UPDATE_MULTIUPDATE_DEFAULT,
                UPDATE_UPSERT_DEFAULT, durability);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void updateAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update)
            throws MongoDbException {
        updateAsync(new LambdaCallbackAdapter<Long>(results), query, update);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void updateAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final boolean multiUpdate, final boolean upsert)
            throws MongoDbException {
        updateAsync(new LambdaCallbackAdapter<Long>(results), query, update,
                multiUpdate, upsert);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void updateAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final boolean multiUpdate, final boolean upsert,
            final Durability durability) throws MongoDbException {
        updateAsync(new LambdaCallbackAdapter<Long>(results), query, update,
                multiUpdate, upsert, durability);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #updateAsync(Callback, DocumentAssignable, DocumentAssignable, Durability)}
     * method with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void updateAsync(final LambdaCallback<Long> results,
            final DocumentAssignable query, final DocumentAssignable update,
            final Durability durability) throws MongoDbException {
        updateAsync(new LambdaCallbackAdapter<Long>(results), query, update,
                durability);
    }

    /**
     * Returns the type of lock to use.
     * 
     * @return The type of lock to use.
     */
    protected LockType getLockType() {
        return myClient.getConfig().getLockType();
    }

}
