/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.IntegerElement;

/**
 * Find provides an immutable container for all of the options for a query.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Find {

    /**
     * If set to true requests for data will block, waiting for data. Useful
     * with {@link Builder#tailable()} cursors.
     */
    private final boolean myAwaitData;

    /** The number of documents to be returned in each batch of results. */
    private final int myBatchSize;

    /** The hint for which index to use. */
    private final Document myHint;

    /** The hint for which index to use by name. */
    private final String myHintName;

    /**
     * If set to true the cursor returned from the query will not timeout or die
     * automatically, e.g., immortal.
     */
    private final boolean myImmortalCursor;

    /** The total number of documents to be returned. */
    private final int myLimit;

    /** The number of documents to skip before returning the first document. */
    private final int myNumberToSkip;

    /** If true then an error in the query should return any partial results. */
    private final boolean myPartialOk;

    /** The query document. */
    private final Document myQuery;

    /** The preference for which servers to use to retrieve the results. */
    private final ReadPreference myReadPreference;

    /** The fields to be returned from the matching documents. */
    private final Document myReturnFields;

    /**
     * If set to true then use snapshot mode to ensure document are only
     * returned once.
     */
    private final boolean mySnapshot;

    /** The fields to order the document by. */
    private final Document mySort;

    /** If set to true the cursor returned from the query will be tailable. */
    private final boolean myTailable;

    /**
     * Creates a new Find.
     * 
     * @param builder
     *            The builder to copy the query fields from.
     */
    protected Find(final Builder builder) {
        myBatchSize = builder.myBatchSize;
        myHint = builder.myHint;
        myHintName = builder.myHintName;
        myLimit = builder.myLimit;
        myNumberToSkip = builder.myNumberToSkip;
        myPartialOk = builder.myPartialOk;
        myQuery = builder.myQuery;
        myReadPreference = builder.myReadPreference;
        myReturnFields = builder.myReturnFields;
        mySnapshot = builder.mySnapshot;
        mySort = builder.mySort;
        myTailable = builder.myTailable;
        myAwaitData = builder.myAwaitData;
        myImmortalCursor = builder.myImmortalCursor;
    }

    /**
     * Returns the number of documents to be returned in each batch of results.
     * 
     * @return The number of documents to be returned in each batch of results.
     */
    public int getBatchSize() {
        return myBatchSize;
    }

    /**
     * Returns the hint for which index to use.
     * 
     * @return The hint for which index to use.
     */
    public Document getHint() {
        return myHint;
    }

    /**
     * Returns the hint for which index to use by name.
     * 
     * @return The hint for which index to use by name.
     */
    public String getHintName() {
        return myHintName;
    }

    /**
     * Returns the total number of documents to be returned.
     * 
     * @return The total number of documents to be returned.
     */
    public int getLimit() {
        return myLimit;
    }

    /**
     * Returns the number of documents to skip before returning the first
     * document.
     * 
     * @return The number of documents to skip before returning the first
     *         document.
     */
    public int getNumberToSkip() {
        return myNumberToSkip;
    }

    /**
     * Returns the query document.
     * 
     * @return The query document.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the preference for the servers to retrieve the results from. May
     * be <code>null</code> in which case the default read preference should be
     * used.
     * 
     * @return The preference for the servers to retrieve the results from.
     */
    public ReadPreference getReadPreference() {
        return myReadPreference;
    }

    /**
     * Returns the fields to be returned from the matching documents.
     * 
     * @return The fields to be returned from the matching documents.
     */
    public Document getReturnFields() {
        return myReturnFields;
    }

    /**
     * Returns the fields to order document by.
     * 
     * @return The fields to order document by.
     */
    public Document getSort() {
        return mySort;
    }

    /**
     * Returns true if the cursor returned from the query will block or wait for
     * data. This is mainly useful with {@link Builder#tailable()} cursors.
     * 
     * @return True if the cursor returned from the query will block or wait for
     *         data.
     */
    public boolean isAwaitData() {
        return myAwaitData;
    }

    /**
     * Returns true if the cursor returned from the query will not timeout or
     * die automatically, e.g., immortal, false otherwise.
     * 
     * @return True if the cursor returned from the query will not timeout or
     *         die automatically, e.g., immortal.
     * @see Builder#setImmortalCursor(boolean)
     *      Find.Builder.setimmortalCursor(boolean) for important usage
     *      information.
     */
    public boolean isImmortalCursor() {
        return myImmortalCursor;
    }

    /**
     * Returns the partial okay value. If true then an error in the query should
     * return any partial results.
     * 
     * @return The partial okay value. If true then an error in the query should
     *         return any partial results.
     */
    public boolean isPartialOk() {
        return myPartialOk;
    }

    /**
     * If returns true then use snapshot mode to ensure document are only
     * returned once.
     * 
     * @return True then use snapshot mode to ensure document are only returned
     *         once.
     */
    public boolean isSnapshot() {
        return mySnapshot;
    }

    /**
     * Returns true if the cursor returned from the query will be tailable,
     * false otherwise.
     * 
     * @return True if the cursor returned from the query will be tailable,
     *         false otherwise.
     * @see Builder#setTailable(boolean) Find.Builder.setTailable(boolean) for
     *      important usage information.
     */
    public boolean isTailable() {
        return myTailable;
    }

    /**
     * This method is not intended for applications to use. Applications should
     * pass the {@link Find} object to the appropriate method on the
     * {@link MongoCollection} interface. This method is used internally by the
     * driver and is public for cross package access only.
     * <p>
     * Converts the {@link Find} into a raw query request document to send to
     * the MongoDB server.
     * </p>
     * 
     * @param explain
     *            If true then explain the query procedure instead of returning
     *            results.
     * @return The query request document to send to the MongoDB server.
     */
    public Document toQueryRequest(final boolean explain) {
        return toQueryRequest(explain, null);
    }

    /**
     * This method is not intended for applications to use. Applications should
     * pass the {@link Find} object to the appropriate method on the
     * {@link MongoCollection} interface. This method is used internally by the
     * driver and is public for cross package access only.
     * <p>
     * Converts the {@link Find} into a raw query request document to send to
     * the MongoDB server including the provided read preferences.
     * </p>
     * 
     * @param explain
     *            If true then explain the query procedure instead of returning
     *            results.
     * @param readPreference
     *            The read preference to include in the query request document.
     * @return The query request document to send to the MongoDB server.
     */
    public Document toQueryRequest(final boolean explain,
            final ReadPreference readPreference) {

        if (explain || mySnapshot || (mySort != null) || (myHint != null)
                || (myHintName != null) || (readPreference != null)) {
            final DocumentBuilder builder = BuilderFactory.start();

            builder.add("query", myQuery);

            if (mySort != null) {
                builder.add("orderby", mySort);
            }

            if (myHint != null) {
                builder.add("$hint", myHint);
            }
            else if (myHintName != null) {
                builder.add("$hint", myHintName);
            }

            if (explain) {
                builder.add("$explain", true);
            }

            if (mySnapshot) {
                builder.add("$snapshot", true);
            }

            if (readPreference != null) {
                builder.add(ReadPreference.FIELD_NAME, readPreference);
            }

            return builder.build();
        }

        return myQuery;
    }

    /**
     * Helper for creating immutable {@link Find} queries.
     * 
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {
        /**
         * If set to true requests for data will block, waiting for data. Useful
         * with {@link #tailable()} cursors.
         */
        protected boolean myAwaitData;

        /** The number of documents to be returned in each batch of results. */
        protected int myBatchSize;

        /** The hint for which index to use. */
        protected Document myHint;

        /** The hint for which index to use. */
        protected String myHintName;

        /**
         * If set to true the cursor returned from the query will not timeout or
         * die automatically, e.g., immortal.
         */
        protected boolean myImmortalCursor;

        /** The total number of documents to be returned. */
        protected int myLimit;

        /** The number of documents to skip before returning the first document. */
        protected int myNumberToSkip;

        /**
         * If true then an error in the query should return any partial results.
         */
        protected boolean myPartialOk;

        /** The query document. */
        protected Document myQuery;

        /** The preference for which servers to use to retrieve the results. */
        protected ReadPreference myReadPreference;

        /** The fields to be returned from the matching documents. */
        protected Document myReturnFields;

        /**
         * If set to true then use snapshot mode to ensure document are only
         * returned once.
         */
        protected boolean mySnapshot;

        /** The fields to order the document on. */
        protected Document mySort;

        /** If set to true the cursor returned from the query will be tailable. */
        protected boolean myTailable;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Creates a new Builder.
         * 
         * @param query
         *            The query document.
         */
        public Builder(final DocumentAssignable query) {
            this();
            myQuery = query.asDocument();
        }

        /**
         * Constructs a new {@link Find} object from the state of the builder.
         * 
         * @return The new {@link Find} object.
         */
        public Find build() {
            return new Find(this);
        }

        /**
         * Sets that if there is an error then the query should return any
         * partial results.
         * 
         * @return This builder for chaining method calls.
         */
        public Builder partialOk() {
            return setPartialOk(true);
        }

        /**
         * Resets the builder back to its initial state for reuse.
         * 
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myBatchSize = 0;
            myHint = null;
            myHintName = null;
            myLimit = 0;
            myNumberToSkip = 0;
            myPartialOk = false;
            myQuery = null;
            myReadPreference = null;
            myReturnFields = null;
            mySnapshot = false;
            mySort = null;
            myTailable = false;
            myAwaitData = false;
            myImmortalCursor = false;

            return this;
        }

        /**
         * If set to true requests for data will block, waiting for data. Useful
         * with {@link #tailable()} cursors.
         * 
         * @param awaitData
         *            True if requests for data will block, waiting for data.
         *            Useful with {@link #tailable()} cursors.
         * @return This builder for chaining method calls.
         */
        public Builder setAwaitData(final boolean awaitData) {
            myAwaitData = awaitData;
            return this;
        }

        /**
         * Sets the value of the number of documents to be returned in each
         * batch.
         * 
         * @param batchSize
         *            The new value for the number of documents to be returned
         *            in each batch.
         * @return This builder for chaining method calls.
         */
        public Builder setBatchSize(final int batchSize) {
            myBatchSize = batchSize;
            return this;
        }

        /**
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * 
         * @param indexFields
         *            The new value for the fields of the index to use to
         *            execute the query.
         * @return This builder for chaining method calls.
         */
        public Builder setHint(final DocumentAssignable indexFields) {
            myHintName = null;
            myHint = indexFields.asDocument();
            return this;
        }

        /**
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * <p>
         * This method is intended to be used with the {@link Sort} class's
         * static methods: <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link Sort#asc(String) com.allanbank.mongodb.builder.Sort.asc};
         * import static {@link Sort#desc(String) com.allanbank.mongodb.builder.Sort.desc};
         * 
         * Find.Builder builder = new Find.Builder();
         * 
         * builder.setHint( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param indexFields
         *            The new value for the fields of the index to use to
         *            execute the query.
         * @return This builder for chaining method calls.
         */
        public Builder setHint(final IntegerElement... indexFields) {
            final DocumentBuilder builder = BuilderFactory.start();
            for (final IntegerElement sortField : indexFields) {
                builder.add(sortField);
            }
            myHintName = null;
            myHint = builder.build();
            return this;
        }

        /**
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * 
         * @param indexName
         *            The new value for the name of the index to use to execute
         *            the query.
         * @return This builder for chaining method calls.
         */
        public Builder setHint(final String indexName) {
            myHintName = indexName;
            myHint = null;
            return this;
        }

        /**
         * If set to true the cursor returned from the query will not timeout or
         * die automatically, e.g., immortal. The user most either exhaust the
         * results of the query or explicitly close the {@link MongoIterator} or
         * {@link MongoCursorControl} returned.
         * <p>
         * Under normal circumstances using an immortal cursor is not needed and
         * its repeated incorrect usage could cause a memory leak on the MongoDB
         * server and impact performance. Extreme caution should be used to
         * ensure the number of active cursors on the server does not grow
         * without bounds.
         * </p>
         * 
         * @param immortal
         *            True if the cursor returned from the query should be
         *            immortal.
         * @return This builder for chaining method calls.
         */
        public Builder setImmortalCursor(final boolean immortal) {
            myImmortalCursor = immortal;
            return this;
        }

        /**
         * Sets the value of the total number of documents to be returned.
         * 
         * @param limit
         *            The new value for the total number of documents to be
         *            returned.
         * @return This builder for chaining method calls.
         */
        public Builder setLimit(final int limit) {
            myLimit = limit;
            return this;
        }

        /**
         * Sets the value of the number of documents to skip before returning
         * the first document to the new value.
         * 
         * @param numberToSkip
         *            The new value for the number of documents to skip before
         *            returning the first document.
         * @return This builder for chaining method calls.
         */
        public Builder setNumberToSkip(final int numberToSkip) {
            myNumberToSkip = numberToSkip;
            return this;
        }

        /**
         * Sets the value of partial okay to the new value. If true then an
         * error in the query should return any partial results.
         * 
         * @param partialOk
         *            The new value for the partial okay.
         * @return This builder for chaining method calls.
         */
        public Builder setPartialOk(final boolean partialOk) {
            myPartialOk = partialOk;
            return this;
        }

        /**
         * Sets the value of the query document to the new value.
         * 
         * @param query
         *            The new value for the query document.
         * @return This builder for chaining method calls.
         */
        public Builder setQuery(final DocumentAssignable query) {
            myQuery = query.asDocument();
            return this;
        }

        /**
         * Sets the preference for the set of servers to retrieve the results
         * from.
         * 
         * @param readPreference
         *            The new value for the preference of which server to return
         *            the results from.
         * @return This builder for chaining method calls.
         */
        public Builder setReadPreference(final ReadPreference readPreference) {
            myReadPreference = readPreference;
            return this;
        }

        /**
         * Sets the value of the fields to be returned from the matching
         * documents to the new value.
         * 
         * @param returnFields
         *            The new value for the fields to be returned from the
         *            matching documents.
         * @return This builder for chaining method calls.
         */
        public Builder setReturnFields(final DocumentAssignable returnFields) {
            myReturnFields = returnFields.asDocument();
            return this;
        }

        /**
         * Sets the value of snapshot to the new value. If set to true then use
         * snapshot mode to ensure document are only returned once.
         * 
         * @param snapshot
         *            The new value for the partial okay.
         * @return This builder for chaining method calls.
         */
        public Builder setSnapshot(final boolean snapshot) {
            mySnapshot = snapshot;
            return this;
        }

        /**
         * Sets the value of the fields to to sort matching documents by.
         * 
         * @param sortFields
         *            The new value for the fields to sort matching documents
         *            by.
         * @return This builder for chaining method calls.
         */
        public Builder setSort(final DocumentAssignable sortFields) {
            mySort = sortFields.asDocument();
            return this;
        }

        /**
         * Sets the value of the fields to to sort matching documents by.
         * <p>
         * This method is intended to be used with the {@link Sort} class's
         * static methods: <blockquote>
         * 
         * <pre>
         * <code>
         * import static {@link Sort#asc(String) com.allanbank.mongodb.builder.Sort.asc};
         * import static {@link Sort#desc(String) com.allanbank.mongodb.builder.Sort.desc};
         * 
         * Find.Builder builder = new Find.Builder();
         * 
         * builder.setSort( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         * 
         * </blockquote>
         * 
         * @param sortFields
         *            The new value for the fields to sort matching documents
         *            by.
         * @return This builder for chaining method calls.
         */
        public Builder setSort(final IntegerElement... sortFields) {
            final DocumentBuilder builder = BuilderFactory.start();
            for (final IntegerElement sortField : sortFields) {
                builder.add(sortField);
            }
            mySort = builder.build();
            return this;
        }

        /**
         * If set to true the cursor returned from the query will be tailable.
         * <p>
         * Testing has shown that a tailable cursor on an empty collection will
         * not setup a cursor on the MongoDB server and will immediately return
         * false from {@link MongoIterator#hasNext()}.
         * </p>
         * <p>
         * When using a tailable cursor that has exhausted the available data
         * will cause the {@link MongoIterator#hasNext()} calls to block until
         * more data is available. The connection that is used to request more
         * documents will also be blocked for short intervals (measured to be
         * ~2.25 seconds with 2.0.7). Any requests submitted behind the cursors
         * request will also be blocked.
         * </p>
         * <p>
         * It is highly recommended that the number of connections within the
         * {@link MongoClientConfiguration} be at least 1 more than the maximum
         * number of active tailable cursors.
         * </p>
         * 
         * @param tailable
         *            The new value for if the cursor returned from the query
         *            will be tailable.
         * @return This builder for chaining method calls.
         */
        public Builder setTailable(final boolean tailable) {
            myTailable = tailable;

            return this;
        }

        /**
         * Sets that the query should ensure that documents are only returned
         * once.
         * 
         * @return This builder for chaining method calls.
         */
        public Builder snapshot() {
            return setSnapshot(true);
        }

        /**
         * Sets the the cursor returned from the query to be
         * {@link #setTailable(boolean) setTailable(true)} and
         * {@link #setAwaitData(boolean) setAwaitData(true)}.
         * 
         * @return This builder for chaining method calls.
         * @see #setTailable(boolean) setTailable(boolean) for important usage
         *      information.
         */
        public Builder tailable() {
            return setTailable(true).setAwaitData(true);
        }
    }
}
