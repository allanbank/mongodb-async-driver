/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
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
 * Interface for interacting with a MongoDB collection.
 * <p>
 * The asynchronous methods for interacting with a collection are declared as
 * part of the {@link AsyncMongoCollection} interface (which this interface
 * extends. The separation of the interfaces is to support the ability to batch
 * requests to the server. This will take advantage of the batched write
 * commands if the driver is only connected to @link {@link Version#VERSION_2_6
 * 2.6} or above servers otherwise batching is similar in function to the
 * {@link MongoClient#asSerializedClient() serialized client} capability.
 * </p>
 * <p>
 * To use the batching capability you will need to call the
 * {@link #startBatch()} method and then ensure that the
 * {@link BatchedAsyncMongoCollection#close() close()} method is called to
 * submit the batch of requests.
 * </p>
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoCollection extends AsyncMongoCollection {
    /** An (empty) query document to find all documents. */
    public static final Document ALL = EmptyDocument.INSTANCE;

    /** An (empty) update document to perform no actual modifications. */
    public static final Document NONE = MongoCollection.ALL;

    /**
     * Invokes a aggregate command on the server.
     * 
     * @param command
     *            The details of the aggregation request.
     * @return The aggregation results returned.
     * @throws MongoDbException
     *             On an error executing the aggregate command.
     */
    public MongoIterator<Document> aggregate(Aggregate command)
            throws MongoDbException;

    /**
     * Invokes a aggregate command on the server.
     * 
     * @param command
     *            The details of the aggregation request.
     * @return The aggregation results returned.
     * @throws MongoDbException
     *             On an error executing the aggregate command.
     */
    public MongoIterator<Document> aggregate(Aggregate.Builder command)
            throws MongoDbException;

    /**
     * Counts the set of documents in the collection.
     * <p>
     * This is equivalent to calling {@link #countAsync() countAsync().get()}
     * </p>
     * 
     * @return The number of documents in the collection.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count() throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param count
     *            The count command.
     * @return The count of the documents.
     * @throws MongoDbException
     *             On an error counting the documents.
     */
    public long count(Count count) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param count
     *            The count command.
     * @return The count of the documents.
     * @throws MongoDbException
     *             On an error counting the documents.
     */
    public long count(Count.Builder count) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * <p>
     * This is equivalent to calling {@link #countAsync(DocumentAssignable)
     * countAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query document.
     * @return The number of matching documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count(DocumentAssignable query) throws MongoDbException;

    /**
     * Counts the set of documents matching the query document in the
     * collection.
     * 
     * @param query
     *            The query document.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @return The number of matching documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count(DocumentAssignable query, ReadPreference readPreference)
            throws MongoDbException;

    /**
     * Counts the set of documents in the collection.
     * <p>
     * This is equivalent to calling {@link #countAsync() countAsync().get()}
     * </p>
     * 
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @return The number of documents in the collection.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public long count(ReadPreference readPreference) throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified and if
     * <tt>unique</tt> is true ensuring entries are unique.
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.createIndex( true, asc("f"), desc("g") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param unique
     *            If true then the index created will enforce entries are
     *            unique.
     * @param keys
     *            The keys to use for the index.
     * @throws MongoDbException
     *             On a failure building the index.
     */
    public void createIndex(boolean unique, Element... keys)
            throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified
     * allowing duplicate entries.
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.bson.builder.BuilderFactory#start com.allanbank.mongodb.bson.builder.BuilderFactory.start};
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.createIndex(start().add("sparse", true), asc("f") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param options
     *            The options for the index.
     * @param keys
     *            The keys to use for the index.
     * @throws MongoDbException
     *             On a failure building the index.
     * @see <a
     *      href="http://www.mongodb.org/display/DOCS/Indexes#Indexes-CreationOptions">Index
     *      Options Documentation</a>
     */
    public void createIndex(DocumentAssignable options, Element... keys)
            throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified
     * allowing duplicate entries.
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.createIndex( asc("f"), desc("g") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param keys
     *            The keys to use for the index.
     * @throws MongoDbException
     *             On a failure building the index.
     */
    public void createIndex(Element... keys) throws MongoDbException;

    /**
     * Creates an index with the specified name, across the keys specified and
     * if <tt>unique</tt> is true ensuring entries are unique.
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.createIndex( "f_and_g", false, asc("f"), desc("g") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
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
    public void createIndex(String name, boolean unique, Element... keys)
            throws MongoDbException;

    /**
     * Creates an index with a generated name, across the keys specified
     * allowing duplicate entries.
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.bson.builder.BuilderFactory#start com.allanbank.mongodb.bson.builder.BuilderFactory.start};
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.createIndex("sparse_f", start().add("sparse", true), asc("f") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param name
     *            The name of the index. If <code>null</code> then a name is
     *            generated based on the keys.
     * @param options
     *            The options for the index.
     * @param keys
     *            The keys to use for the index.
     * @throws MongoDbException
     *             On a failure building the index.
     * @see <a
     *      href="http://www.mongodb.org/display/DOCS/Indexes#Indexes-CreationOptions">Index
     *      Options Documentation</a>
     */
    public void createIndex(String name, DocumentAssignable options,
            Element... keys) throws MongoDbException;

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
    public long delete(DocumentAssignable query) throws MongoDbException;

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
    public long delete(DocumentAssignable query, boolean singleDelete)
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
    public long delete(DocumentAssignable query, boolean singleDelete,
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
    public long delete(DocumentAssignable query, Durability durability)
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
    public MongoIterator<Element> distinct(Distinct command)
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
    public MongoIterator<Element> distinct(Distinct.Builder command)
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
     * <p>
     * This method is intended to be used with the
     * {@link com.allanbank.mongodb.builder.Index} class's static methods:
     * <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
     * import static {@link com.allanbank.mongodb.builder.Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
     * 
     * MongoCollection collection = ...;
     * 
     * collection.dropIndex( asc("f"), desc("g") );
     * ...
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param keys
     *            The keys for the index to be dropped.
     * @return If any indexes were removed.
     * @throws MongoDbException
     *             On an error deleting the indexes.
     */
    public boolean dropIndex(IntegerElement... keys) throws MongoDbException;

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
     * Explains the way that the aggregation will be performed.
     * <p>
     * This is equivalent to calling {@link #explainAsync(Aggregate)
     * explainAsync(...).get()}
     * </p>
     * 
     * @param aggregation
     *            The aggregation details.
     * @return The document describing the method used to execute the
     *         aggregation.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @since MongoDB 2.6
     */
    public Document explain(Aggregate aggregation) throws MongoDbException;

    /**
     * Explains the way that the aggregation will be performed.
     * <p>
     * This is equivalent to calling {@link #explainAsync(Aggregate)
     * explainAsync(...).get()}
     * </p>
     * 
     * @param aggregation
     *            The aggregation details.
     * @return The document describing the method used to execute the
     *         aggregation.
     * @throws MongoDbException
     *             On an error finding the documents.
     * @since MongoDB 2.6
     */
    public Document explain(Aggregate.Builder aggregation)
            throws MongoDbException;

    /**
     * Explains the way that the query will be performed.
     * 
     * @param query
     *            The query document.
     * @return The document describing the method used to execute the query.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Document explain(DocumentAssignable query) throws MongoDbException;

    /**
     * Explains the way that the query will be performed.
     * <p>
     * This is equivalent to calling {@link #explainAsync(Find)
     * explainAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The document describing the method used to execute the query.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Document explain(Find query) throws MongoDbException;

    /**
     * Explains the way that the query will be performed.
     * <p>
     * This is equivalent to calling {@link #explainAsync(Find)
     * explainAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The document describing the method used to execute the query.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Document explain(Find.Builder query) throws MongoDbException;

    /**
     * Finds the set of documents matching the query document in the collection.
     * <p>
     * This is equivalent to calling {@link #findAsync(DocumentAssignable)
     * findAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query document.
     * @return The MongoIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public MongoIterator<Document> find(DocumentAssignable query)
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
     * @return The MongoIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public MongoIterator<Document> find(Find query) throws MongoDbException;

    /**
     * Finds the set of documents matching the query in the collection.
     * <p>
     * This is equivalent to calling {@link #findAsync(Find)
     * findAsync(...).get()}
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The MongoIterator over the documents.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public MongoIterator<Document> find(Find.Builder query)
            throws MongoDbException;

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
     * @param command
     *            The details of the find and modify request.
     * @return The found document.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public Document findAndModify(FindAndModify.Builder command)
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
    public Document findOne(DocumentAssignable query) throws MongoDbException;

    /**
     * Finds a single matching document in the collection.
     * <p>
     * Note that following options in the {@link Find} class do not make sense
     * and are silently ignored by this method.
     * <ul>
     * <li> {@link Find#getBatchSize() Batch Size} - Automatically set to 1.</li>
     * <li> {@link Find#getLimit() Limit} - Automatically set to 1.</li>
     * <li> {@link Find#isTailable() Tailable} - This method only returns 1
     * document.</li>
     * </ul>
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The first found document.
     * @throws MongoDbException
     *             On an error finding the document.
     */
    public Document findOne(Find query) throws MongoDbException;

    /**
     * Finds a single matching document in the collection.
     * <p>
     * Note that following options in the {@link Find} class do not make sense
     * and are silently ignored by this method.
     * <ul>
     * <li> {@link Find#getBatchSize() Batch Size} - Automatically set to 1.</li>
     * <li> {@link Find#getLimit() Limit} - Automatically set to 1.</li>
     * <li> {@link Find#isTailable() Tailable} - This method only returns 1
     * document.</li>
     * </ul>
     * </p>
     * 
     * @param query
     *            The query details.
     * @return The first found document.
     * @throws MongoDbException
     *             On an error finding the document.
     */
    public Document findOne(Find.Builder query) throws MongoDbException;

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getDatabaseName();

    /**
     * Returns the durability for write operations sent to the server from this
     * {@link MongoCollection} instance.
     * <p>
     * Defaults to the {@link Durability} from the parent {@link MongoDatabase}
     * instance.
     * </p>
     * 
     * @return The durability for write operations on the server.
     * 
     * @see MongoDatabase#getDurability()
     */
    public Durability getDurability();

    /**
     * Returns the name of the collection.
     * 
     * @return The name of the collection.
     */
    public String getName();

    /**
     * Returns the read preference for queries from this {@link MongoCollection}
     * instance.
     * <p>
     * Defaults to {@link ReadPreference} from the parent {@link MongoDatabase}
     * instance.
     * </p>
     * 
     * @return The default read preference for a query.
     * 
     * @see MongoDatabase#getReadPreference()
     */
    public ReadPreference getReadPreference();

    /**
     * Invokes a group command on the server.
     * 
     * @param command
     *            The details of the group request.
     * @return The group results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public MongoIterator<Element> groupBy(GroupBy command)
            throws MongoDbException;

    /**
     * Invokes a group command on the server.
     * 
     * @param command
     *            The details of the group request.
     * @return The group results returned.
     * @throws MongoDbException
     *             On an error finding the documents.
     */
    public MongoIterator<Element> groupBy(GroupBy.Builder command)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, DocumentAssignable[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param continueOnError
     *            If the insert should continue if one of the documents causes
     *            an error.
     * @param documents
     *            The documents to add to the collection.
     * @return Currently, returns zero. Once <a
     *         href="http://jira.mongodb.org/browse/SERVER-4381">SERVER-4381</a>
     *         is fixed then expected to return the number of documents
     *         inserted. If the durability is NONE then returns <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final boolean continueOnError,
            DocumentAssignable... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, DocumentAssignable[])
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
     * @return Currently, returns zero. Once <a
     *         href="http://jira.mongodb.org/browse/SERVER-4381">SERVER-4381</a>
     *         is fixed then expected to return the number of documents
     *         inserted. If the durability is NONE then returns <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final boolean continueOnError,
            final Durability durability, DocumentAssignable... documents)
            throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, DocumentAssignable[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param documents
     *            The documents to add to the collection.
     * @return Currently, returns zero. Once <a
     *         href="http://jira.mongodb.org/browse/SERVER-4381">SERVER-4381</a>
     *         is fixed then expected to return the number of documents
     *         inserted. If the durability is NONE then returns <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(DocumentAssignable... documents) throws MongoDbException;

    /**
     * Inserts a set of documents into the collection.
     * <p>
     * This is equivalent to calling
     * {@link #insertAsync(boolean, Durability, DocumentAssignable[])
     * insertAsync(...).get()}
     * </p>
     * 
     * @param durability
     *            The durability for the insert.
     * @param documents
     *            The documents to add to the collection.
     * @return Currently, returns zero. Once <a
     *         href="http://jira.mongodb.org/browse/SERVER-4381">SERVER-4381</a>
     *         is fixed then expected to return the number of documents
     *         inserted. If the durability is NONE then returns <code>-1</code>.
     * @throws MongoDbException
     *             On an error inserting the documents.
     */
    public int insert(final Durability durability,
            DocumentAssignable... documents) throws MongoDbException;

    /**
     * Returns true if the collection {@link #stats() statistics} indicate that
     * the collection is a capped collection.
     * 
     * @return True if the collection {@link #stats() statistics} indicate that
     *         the collection is a capped collection.
     * @throws MongoDbException
     *             On an error collecting the collection statistics.
     */
    public boolean isCapped() throws MongoDbException;

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
    public MongoIterator<Document> mapReduce(MapReduce command)
            throws MongoDbException;

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
    public MongoIterator<Document> mapReduce(MapReduce.Builder command)
            throws MongoDbException;

    /**
     * Saves the {@code document} to the collection.
     * <p>
     * If the {@code document} does not contain an {@code _id} field then this
     * method is equivalent to: {@link #insert(DocumentAssignable...)
     * insert(document)}.
     * </p>
     * <p>
     * If the {@code document} does contain an {@code _id} field then this
     * method is equivalent to:
     * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean)
     * update(BuilderFactory.start().add(document.get("_id")), document, false,
     * true)}.
     * </p>
     * 
     * @param document
     *            The document to save to the collection.
     * @return ListenableFuture that will be updated with the results of the
     *         save. If the durability of the operation is NONE then this will
     *         be -1.
     * @throws MongoDbException
     *             On an error saving the documents.
     */
    public int save(DocumentAssignable document) throws MongoDbException;

    /**
     * Saves the {@code document} to the collection.
     * <p>
     * If the {@code document} does not contain an {@code _id} field then this
     * method is equivalent to:
     * {@link #insert(Durability, DocumentAssignable...) insert(durability,
     * document)}.
     * </p>
     * <p>
     * If the {@code document} does contain an {@code _id} field then this
     * method is equivalent to:
     * {@link #update(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)
     * update(BuilderFactory.start().add(document.get("_id")), document, false,
     * true, durability)}.
     * </p>
     * 
     * @param document
     *            The document to save to the collection.
     * @param durability
     *            The durability for the save.
     * @return ListenableFuture that will be updated with the results of the
     *         save. If the durability of the operation is NONE then this will
     *         be -1.
     * @throws MongoDbException
     *             On an error saving the documents.
     */
    public int save(DocumentAssignable document, Durability durability)
            throws MongoDbException;

    /**
     * Sets the durability for write operations from this
     * {@link MongoCollection} instance.
     * <p>
     * Defaults to the {@link Durability} from the parent {@link MongoDatabase}
     * instance if set to <code>null</code>.
     * </p>
     * 
     * @param durability
     *            The durability for write operations on the server.
     * 
     * @see MongoDatabase#getDurability()
     */
    public void setDurability(final Durability durability);

    /**
     * Sets the value of the read preference for a queries from this
     * {@link MongoCollection} instance.
     * <p>
     * Defaults to the {@link ReadPreference} from the parent
     * {@link MongoDatabase} instance if set to <code>null</code>.
     * </p>
     * 
     * @param readPreference
     *            The read preference for a query.
     * 
     * @see MongoDatabase#getReadPreference()
     */
    public void setReadPreference(final ReadPreference readPreference);

    /**
     * Starts a batch of requests to the server. The returned object will not
     * submit any requests to the server until the batch is closed.
     * 
     * @return The interface for submitting batched requests. The
     *         {@link BatchedAsyncMongoCollection#close()} method must be called
     *         to submit the batch of requests.
     */
    public BatchedAsyncMongoCollection startBatch();

    /**
     * Returns the statistics for the collection.
     * 
     * @return The results document with the collection statistics.
     * @throws MongoDbException
     *             On an error collecting the collection statistics.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/collStats/">collStats
     *      Command Reference</a>
     */
    public Document stats() throws MongoDbException;

    /**
     * Invokes a {@code text} command on the server.
     * 
     * @param command
     *            The details of the {@code text} request.
     * @return The {@code text} results returned.
     * @throws MongoDbException
     *             On an error executing the {@code text} command.
     * @see <a
     *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-queries">
     *      MongoDB Text Queries</a>
     * @since MongoDB 2.4
     */
    public MongoIterator<TextResult> textSearch(Text command)
            throws MongoDbException;

    /**
     * Invokes a {@code text} command on the server.
     * 
     * @param command
     *            The details of the {@code text} request.
     * @return The {@code text} results returned.
     * @throws MongoDbException
     *             On an error executing the {@code text} command.
     * @see <a
     *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-queries">
     *      MongoDB Text Queries</a>
     * @since MongoDB 2.4
     */
    public MongoIterator<TextResult> textSearch(Text.Builder command)
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
    public long update(DocumentAssignable query, DocumentAssignable update)
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
     * @return The number of documents updated. If the durability of the
     *         operation is NONE then this will be -1.
     * @throws MongoDbException
     *             On an error updating the documents.
     */
    public long update(DocumentAssignable query, DocumentAssignable update,
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
    public long update(DocumentAssignable query, DocumentAssignable update,
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
    public long update(DocumentAssignable query, DocumentAssignable update,
            final Durability durability) throws MongoDbException;

    /**
     * Updates the collection's options or flags using the {@code collMod}
     * command. The return value is the response from the MongoDB server and
     * normally contains a <code>&lt;name&gt;_old</code> field for each
     * successfully set option on the collection. <blockquote>
     * 
     * <pre>
     * <code>
     * MongoCollection collection = ...;
     * 
     * collection.updateOptions( BuilderFactory.start().add( "usePowerOf2Sizes", true ) );
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param options
     *            The collection options to be set.
     * @return The results document from the database.
     * @throws MongoDbException
     *             On an error validating the collection.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/collMod/">collMod
     *      Command Reference</a>
     */
    public Document updateOptions(DocumentAssignable options)
            throws MongoDbException;

    /**
     * Validates the collections contents.
     * 
     * @param mode
     *            The validation mode to use.
     * @return The results document from the database.
     * @throws MongoDbException
     *             On an error validating the collection.
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/validate/">validate
     *      Command Reference</a>
     */
    public Document validate(ValidateMode mode) throws MongoDbException;

    /**
     * ValidateMode provides an enumeration of the validation modes.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static enum ValidateMode {

        /** Validates the data and indexes performing all checks. */
        FULL,

        /** Validates the indexes only and not the collection data. */
        INDEX_ONLY,

        /** Validates the data and indexes but skips some checks. */
        NORMAL;
    }
}
