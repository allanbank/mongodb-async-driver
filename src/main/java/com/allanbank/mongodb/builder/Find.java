/*
 * #%L
 * Find.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.builder;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
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
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class Find {
    /** An (empty) query document to find all documents. */
    public static final Document ALL = MongoCollection.ALL;

    /**
     * The first version of MongoDB to support the queries with the ability to
     * limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Version.parse("2.5.4");

    /**
     * Creates a new builder for a {@link Find}.
     *
     * @return The builder to construct a {@link Find}.
     */
    public static Builder builder() {
        return new Builder();
    }

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

    /**
     * If set then controls the maximum number of documents that will be scanned
     * for results.
     */
    private final long myMaximumDocumentsToScan;

    /**
     * If set then controls the maximum value for the range within the used
     * index.
     */
    private final Document myMaximumRange;

    /** The maximum amount of time to allow the query to run. */
    private final long myMaximumTimeMilliseconds;

    /**
     * If set then controls the minimum value for the range within the used
     * index.
     */
    private final Document myMinimumRange;

    /** The number of documents to skip before returning the first document. */
    private final int myNumberToSkip;

    /** If true then an error in the query should return any partial results. */
    private final boolean myPartialOk;

    /** The fields to be projected/returned from the matching documents. */
    private final Document myProjection;

    /** The query document. */
    private final Document myQuery;

    /** The preference for which servers to use to retrieve the results. */
    private final ReadPreference myReadPreference;

    /** If set to true then only the index keys will be returned. */
    private final boolean myReturnIndexKeysOnly;

    /**
     * If set to true then a "$diskLoc" entry will be added to every returned
     * document with the disk location information.
     */
    private final boolean myShowDiskLocation;

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
        myProjection = builder.myProjection;
        mySnapshot = builder.mySnapshot;
        mySort = builder.mySort;
        myTailable = builder.myTailable;
        myAwaitData = builder.myAwaitData;
        myImmortalCursor = builder.myImmortalCursor;
        myMaximumRange = builder.myMaximumRange;
        myMinimumRange = builder.myMinimumRange;
        myMaximumDocumentsToScan = builder.myMaximumDocumentsToScan;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
        myReturnIndexKeysOnly = builder.myReturnIndexKeysOnly;
        myShowDiskLocation = builder.myShowDiskLocation;
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
     * Returns a value greater than zero to controls the maximum number of
     * documents that will be scanned for results.
     *
     * @return A value greater than zero to controls the maximum number of
     *         documents that will be scanned for results.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/operator/maxScan/">$maxScan
     *      Documentation</a>
     */
    public long getMaximumDocumentsToScan() {
        return myMaximumDocumentsToScan;
    }

    /**
     * Returns a non-null value to controls the maximum value for the range
     * within the used index.
     *
     * @return A non-null value to controls the maximum value for the range
     *         within the used index.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/operator/max/">$max
     *      Documentation</a>
     */
    public Document getMaximumRange() {
        return myMaximumRange;
    }

    /**
     * Returns the maximum amount of time to allow the query to run on the
     * Server before it is aborted.
     *
     * @return The maximum amount of time to allow the query to run on the
     *         Server before it is aborted.
     *
     * @since MongoDB 2.6
     */
    public long getMaximumTimeMilliseconds() {
        return myMaximumTimeMilliseconds;
    }

    /**
     * Returns a non-null value to controls the minimum value for the range
     * within the used index.
     *
     * @return A non-null value to controls the minimum value for the range
     *         within the used index.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/operator/min/">$min
     *      Documentation</a>
     */
    public Document getMinimumRange() {
        return myMinimumRange;
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
     * Returns the fields to be projected or returned from the matching
     * documents.
     *
     * @return The fields to be projected from the matching documents.
     */
    public Document getProjection() {
        return myProjection;
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
     * @deprecated Replaced with the MongoDB standardized name:
     *             {@link #getProjection() projection}. This method will be
     *             removed on or after the 1.4 release.
     */
    @Deprecated
    public Document getReturnFields() {
        return myProjection;
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
     * Returns true if only the index keys will be returned.
     *
     * @return True if only the index keys will be returned.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/operator/returnKey/">$returnKey
     *      Documentation</a>
     */
    public boolean isReturnIndexKeysOnly() {
        return myReturnIndexKeysOnly;
    }

    /**
     * Returns true if a "$diskLoc" entry will be added to every returned
     * document with the disk location information.
     *
     * @return True if a "$diskLoc" entry will be added to every returned
     *         document with the disk location information.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/operator/returnKey/">$showDiskLoc
     *      Documentation</a>
     */
    public boolean isShowDiskLocation() {
        return myShowDiskLocation;
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

        if (explain || mySnapshot || myReturnIndexKeysOnly
                || myShowDiskLocation || (mySort != null)
                || (myMaximumDocumentsToScan > 0)
                || (myMaximumTimeMilliseconds > 0) || (myHint != null)
                || (myHintName != null) || (readPreference != null)
                || (myMaximumRange != null) || (myMinimumRange != null)) {
            final DocumentBuilder builder = BuilderFactory.start();

            builder.add("$query", myQuery);

            if (explain) {
                builder.add("$explain", true);
            }

            if (myHint != null) {
                builder.add("$hint", myHint);
            }
            else if (myHintName != null) {
                builder.add("$hint", myHintName);
            }

            if (myMaximumRange != null) {
                builder.add("$max", myMaximumRange);
            }

            if (myMaximumTimeMilliseconds > 0) {
                builder.add("$maxTimeMS", myMaximumTimeMilliseconds);
            }

            if (myMaximumDocumentsToScan > 0) {
                builder.add("$maxScan", myMaximumDocumentsToScan);
            }

            if (myMinimumRange != null) {
                builder.add("$min", myMinimumRange);
            }

            if (mySort != null) {
                builder.add("$orderby", mySort);
            }

            if (myReturnIndexKeysOnly) {
                builder.add("$returnKey", true);
            }

            if (myShowDiskLocation) {
                builder.add("$showDiskLoc", true);
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
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    @ThreadSafe
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

        /**
         * If set then controls the maximum number of documents that will be
         * scanned for results.
         */
        protected long myMaximumDocumentsToScan;

        /**
         * If set then controls the maximum value for the range within the used
         * index.
         */
        protected Document myMaximumRange;

        /** The maximum amount of time to allow the query to run. */
        protected long myMaximumTimeMilliseconds;

        /**
         * If set then controls the minimum value for the range within the used
         * index.
         */
        protected Document myMinimumRange;

        /** The number of documents to skip before returning the first document. */
        protected int myNumberToSkip;

        /**
         * If true then an error in the query should return any partial results.
         */
        protected boolean myPartialOk;

        /** The fields to be returned from the matching documents. */
        protected Document myProjection;

        /** The query document. */
        protected Document myQuery;

        /** The preference for which servers to use to retrieve the results. */
        protected ReadPreference myReadPreference;

        /** If set to true then only the index keys will be returned. */
        protected boolean myReturnIndexKeysOnly;

        /**
         * If set to true then a "$diskLoc" entry will be added to every
         * returned document with the disk location information.
         */
        protected boolean myShowDiskLocation;

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
         * Sets the value of the number of documents to be returned in each
         * batch.
         * <p>
         * This method delegates to {@link #setBatchSize(int)}.
         * </p>
         *
         * @param batchSize
         *            The new value for the number of documents to be returned
         *            in each batch.
         * @return This builder for chaining method calls.
         */
        public Builder batchSize(final int batchSize) {
            return setBatchSize(batchSize);
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
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * <p>
         * This method delegates to {@link #setHint(DocumentAssignable)}.
         * </p>
         *
         * @param indexFields
         *            The new value for the fields of the index to use to
         *            execute the query.
         * @return This builder for chaining method calls.
         */
        public Builder hint(final DocumentAssignable indexFields) {
            return setHint(indexFields);
        }

        /**
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * <p>
         * This method delegates to {@link #setHint(Element...)}.
         * </p>
         * <p>
         * This method is intended to be used with the {@link Index} class's
         * static methods: <blockquote>
         *
         * <pre>
         * <code>
         * import static {@link Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
         * import static {@link Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
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
        public Builder hint(final Element... indexFields) {
            return setHint(indexFields);
        }

        /**
         * Sets the value of hint as to which index should be used to execute
         * the query.
         * <p>
         * This method delegates to the {@link #setHint(String)} method.
         * </p>
         *
         * @param indexName
         *            The new value for the name of the index to use to execute
         *            the query.
         * @return This builder for chaining method calls.
         */
        public Builder hint(final String indexName) {
            return setHint(indexName);
        }

        /**
         * Sets the cursor returned from the query to never timeout or die
         * automatically, e.g., immortal.
         * <p>
         * This method delegates to {@link #setImmortalCursor(boolean)
         * setImmortalCursor(true)}. See its JavaDoc for <b>important usage</b>
         * guidelines.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder immortalCursor() {
            return setImmortalCursor(true);
        }

        /**
         * If set to true the cursor returned from the query will not timeout or
         * die automatically, e.g., immortal.
         * <p>
         * This method delegates to {@link #setImmortalCursor(boolean)}. See its
         * JavaDoc <b>important usage</b> guidelines.
         * </p>
         *
         * @param immortal
         *            True if the cursor returned from the query should be
         *            immortal.
         * @return This builder for chaining method calls.
         */
        public Builder immortalCursor(final boolean immortal) {
            return setImmortalCursor(immortal);
        }

        /**
         * Sets the value of the total number of documents to be returned.
         * <p>
         * This method delegates to {@link #setLimit(int)}.
         * </p>
         *
         * @param limit
         *            The new value for the total number of documents to be
         *            returned.
         * @return This builder for chaining method calls.
         */
        public Builder limit(final int limit) {
            return setLimit(limit);
        }

        /**
         * Sets the value of maximum range for the index used to the new value.
         * <p>
         * This method delegates to {@link #setMaximumRange(DocumentAssignable)}
         * .
         * </p>
         *
         * @param maximumRange
         *            The new value for the maximum range for the index used.
         * @return This builder for chaining method calls.
         */
        public Builder max(final DocumentAssignable maximumRange) {
            return setMaximumRange(maximumRange);
        }

        /**
         * Sets the maximum number of milliseconds to allow the query to run
         * before aborting the request on the server.
         * <p>
         * This method equivalent to {@link #setMaximumTimeMilliseconds(long)
         * setMaximumTimeMilliseconds(timeLimitUnits.toMillis(timeLimit)}.
         * </p>
         *
         * @param timeLimit
         *            The new maximum amount of time to allow the query to run.
         * @param timeLimitUnits
         *            The units for the maximum amount of time to allow the
         *            query to run.
         *
         * @return This {@link Builder} for method call chaining.
         *
         * @since MongoDB 2.6
         */
        public Builder maximumTime(final long timeLimit,
                final TimeUnit timeLimitUnits) {
            return setMaximumTimeMilliseconds(timeLimitUnits
                    .toMillis(timeLimit));
        }

        /**
         * Sets the value of maximum number of documents that will be scanned
         * for results to the new value.
         * <p>
         * This method Delegates to {@link #setMaximumDocumentsToScan(long)}.
         * </p>
         *
         * @param maximumDocumentsToScan
         *            The new value for the maximum number of documents that
         *            will be scanned for results.
         * @return This builder for chaining method calls.
         */
        public Builder maxScan(final long maximumDocumentsToScan) {
            return setMaximumDocumentsToScan(maximumDocumentsToScan);
        }

        /**
         * Sets the value of minimum range for the index used to the new value.
         * <p>
         * This method delegates to {@link #setMinimumRange(DocumentAssignable)}
         * .
         * </p>
         *
         * @param minimumRange
         *            The new value for the minimum range for the index used.
         * @return This builder for chaining method calls.
         */
        public Builder min(final DocumentAssignable minimumRange) {
            return setMinimumRange(minimumRange);
        }

        /**
         * Sets that if there is an error then the query should return any
         * partial results.
         * <p>
         * This method delegates to {@link #setPartialOk(boolean)
         * setPartialOk(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder partialOk() {
            return setPartialOk(true);
        }

        /**
         * Sets the value of partial okay to the new value. If true then an
         * error in the query should return any partial results.
         * <p>
         * This method delegates to {@link #setPartialOk(boolean)}.
         * </p>
         *
         * @param partialOk
         *            The new value for the partial okay.
         * @return This builder for chaining method calls.
         */
        public Builder partialOk(final boolean partialOk) {
            return setPartialOk(partialOk);
        }

        /**
         * Sets the value of the fields to be projected from the matching
         * documents to the new value.
         * <p>
         * This method delegates to {@link #setProjection(DocumentAssignable)} .
         * </p>
         *
         * @param projection
         *            The new value for the fields to be projected from the
         *            matching documents.
         * @return This builder for chaining method calls.
         */
        public Builder projection(final DocumentAssignable projection) {
            return setProjection(projection);
        }

        /**
         * Sets the value of the fields to be returned from the matching
         * documents to the new value.
         * <p>
         * This method adds each field to a document with a value of {@code 1}
         * and then delegates to the {@link #setProjection(DocumentAssignable)}
         * method.
         * </p>
         *
         * @param fieldNames
         *            The names of the fields to be returned.
         * @return This builder for chaining method calls.
         */
        public Builder projection(final String... fieldNames) {
            final DocumentBuilder builder = BuilderFactory.start();
            for (final String fieldName : fieldNames) {
                builder.add(fieldName, 1);
            }
            return setProjection(builder);
        }

        /**
         * Sets the value of the query document to the new value.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}.
         * </p>
         *
         * @param query
         *            The new value for the query document.
         * @return This builder for chaining method calls.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Sets the preference for the set of servers to retrieve the results
         * from.
         * <p>
         * This method delegates to {@link #setReadPreference(ReadPreference)}.
         * </p>
         *
         * @param readPreference
         *            The new value for the preference of which server to return
         *            the results from.
         * @return This builder for chaining method calls.
         */
        public Builder readPreference(final ReadPreference readPreference) {
            return setReadPreference(readPreference);
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
            myQuery = ALL;
            myReadPreference = null;
            myProjection = null;
            mySnapshot = false;
            mySort = null;
            myTailable = false;
            myAwaitData = false;
            myImmortalCursor = false;
            myMaximumRange = null;
            myMaximumTimeMilliseconds = 0;
            myMinimumRange = null;
            myMaximumDocumentsToScan = -1;
            myReturnIndexKeysOnly = false;
            myShowDiskLocation = false;

            return this;
        }

        /**
         * Sets the value of the fields to be returned from the matching
         * documents to the new value.
         * <p>
         * This method delegates to {@link #projection(DocumentAssignable)} .
         * </p>
         *
         * @param returnFields
         *            The new value for the fields to be returned from the
         *            matching documents.
         * @return This builder for chaining method calls.
         * @deprecated Replaced with the MongoDB standardized name:
         *             {@link #projection(DocumentAssignable) projection}. This
         *             method will be removed on or after the 1.4 release.
         */
        @Deprecated
        public Builder returnFields(final DocumentAssignable returnFields) {
            return projection(returnFields);
        }

        /**
         * Sets the value of the fields to be returned from the matching
         * documents to the new value.
         * <p>
         * This method delegates to the {@link #projection(String[])} method.
         * </p>
         *
         * @param fieldNames
         *            The names of the fields to be returned.
         * @return This builder for chaining method calls.
         * @deprecated Replaced with the MongoDB standardized name:
         *             {@link #projection(String[]) projection}. This method
         *             will be removed on or after the 1.4 release.
         */
        @Deprecated
        public Builder returnFields(final String... fieldNames) {
            return projection(fieldNames);
        }

        /**
         * Sets that only index keys should be returned.
         * <p>
         * This method delegates to {@link #setReturnIndexKeysOnly(boolean)
         * setReturnIndexKeysOnly(true)}
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder returnKey() {
            return setReturnIndexKeysOnly(true);
        }

        /**
         * Sets the value for if only index keys should be returned to the new
         * value.
         * <p>
         * This method delegates to {@link #setReturnIndexKeysOnly(boolean)}
         * </p>
         *
         * @param returnIndexKeysOnly
         *            The new value for if only index keys should be returned.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/returnKey/">$returnKey
         *      Documentation</a>
         */
        public Builder returnKey(final boolean returnIndexKeysOnly) {
            return setReturnIndexKeysOnly(returnIndexKeysOnly);
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
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/hint/">$hint
         *      Documentation</a>
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
         * This method is intended to be used with the {@link Index} class's
         * static methods: <blockquote>
         *
         * <pre>
         * <code>
         * import static {@link Index#asc(String) com.allanbank.mongodb.builder.Index.asc};
         * import static {@link Index#desc(String) com.allanbank.mongodb.builder.Index.desc};
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
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/hint/">$hint
         *      Documentation</a>
         */
        public Builder setHint(final Element... indexFields) {
            final DocumentBuilder builder = BuilderFactory.start();
            for (final Element sortField : indexFields) {
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
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/hint/">$hint
         *      Documentation</a>
         */
        public Builder setHint(final String indexName) {
            myHintName = indexName;
            myHint = null;
            return this;
        }

        /**
         * If set to true the cursor returned from the query will not timeout or
         * die automatically, e.g., immortal. The user must either exhaust the
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
         * Sets the value of maximum number of documents that will be scanned
         * for results to the new value.
         * <p>
         * If set to a value greater than zero then controls the maximum number
         * of documents that will be scanned for results.
         * </p>
         *
         * @param maximumDocumentsToScan
         *            The new value for the maximum number of documents that
         *            will be scanned for results.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/maxScan/">$maxScan
         *      Documentation</a>
         */
        public Builder setMaximumDocumentsToScan(
                final long maximumDocumentsToScan) {
            myMaximumDocumentsToScan = maximumDocumentsToScan;
            return this;
        }

        /**
         * Sets the value of maximum range for the index used to the new value.
         * <p>
         * If set then controls the maximum value for the range within the used
         * index.
         * </p>
         *
         * @param maximumRange
         *            The new value for the maximum range for the index used.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/max/">$max
         *      Documentation</a>
         */
        public Builder setMaximumRange(final DocumentAssignable maximumRange) {
            if (maximumRange != null) {
                myMaximumRange = maximumRange.asDocument();
            }
            else {
                myMaximumRange = null;
            }
            return this;
        }

        /**
         * Sets the maximum number of milliseconds to allow the query to run
         * before aborting the request on the server.
         *
         * @param maximumTimeMilliseconds
         *            The new maximum number of milliseconds to allow the query
         *            to run.
         * @return This {@link Builder} for method call chaining.
         *
         * @since MongoDB 2.6
         */
        public Builder setMaximumTimeMilliseconds(
                final long maximumTimeMilliseconds) {
            myMaximumTimeMilliseconds = maximumTimeMilliseconds;
            return this;
        }

        /**
         * Sets the value of minimum range for the index used to the new value.
         * <p>
         * If set then controls the minimum value for the range within the used
         * index.
         * </p>
         *
         * @param minimumRange
         *            The new value for the minimum range for the index used.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/min/">$min
         *      Documentation</a>
         */
        public Builder setMinimumRange(final DocumentAssignable minimumRange) {
            if (minimumRange != null) {
                myMinimumRange = minimumRange.asDocument();
            }
            else {
                myMinimumRange = null;
            }
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
         * Sets the value of the fields to be projected or returned from the
         * matching documents to the new value.
         *
         * @param projection
         *            The new value for the fields to be projected from the
         *            matching documents.
         * @return This builder for chaining method calls.
         */
        public Builder setProjection(final DocumentAssignable projection) {
            myProjection = projection.asDocument();
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
         * <p>
         * This method delegates to {@link #setProjection(DocumentAssignable)} .
         * </p>
         *
         * @param returnFields
         *            The new value for the fields to be returned from the
         *            matching documents.
         * @return This builder for chaining method calls.
         * @deprecated Replaced with the MongoDB standardized name:
         *             {@link #setProjection projection}. This method will be
         *             removed on or after the 1.4 release.
         */
        @Deprecated
        public Builder setReturnFields(final DocumentAssignable returnFields) {
            return setProjection(returnFields);
        }

        /**
         * Sets the value for if only index keys should be returned to the new
         * value.
         * <p>
         * If set to true then only the index keys will be returned.
         * </p>
         *
         * @param returnIndexKeysOnly
         *            The new value for if only index keys should be returned.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/returnKey/">$returnKey
         *      Documentation</a>
         */
        public Builder setReturnIndexKeysOnly(final boolean returnIndexKeysOnly) {
            myReturnIndexKeysOnly = returnIndexKeysOnly;
            return this;
        }

        /**
         * Sets the value if the disk location for each document should be
         * returned to the new value.
         * <p>
         * If set to true then a "$diskLoc" entry will be added to every
         * returned document with the disk location information.
         * </p>
         *
         * @param showDiskLocation
         *            The new value for the if the disk location for each
         *            document should be returned.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/returnKey/">$showDiskLoc
         *      Documentation</a>
         */
        public Builder setShowDiskLocation(final boolean showDiskLocation) {
            myShowDiskLocation = showDiskLocation;
            return this;
        }

        /**
         * Sets the value of snapshot to the new value. If set to true then use
         * snapshot mode to ensure document are only returned once.
         *
         * @param snapshot
         *            The new value for the partial okay.
         * @return This builder for chaining method calls.
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/snapshot/">$snapshot
         *      Documentation</a>
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
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/orderby/">$orderby
         *      Documentation</a>
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
         *
         * @see <a
         *      href="http://docs.mongodb.org/manual/reference/operator/orderby/">$orderby
         *      Documentation</a>
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
         * Sets that the disk location for each document should be returned.
         * <p>
         * This method delegates to {@link #setShowDiskLocation(boolean)
         * setShowDiskLocation(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder showDiskLoc() {
            return setShowDiskLocation(true);
        }

        /**
         * Sets the value if the disk location for each document should be
         * returned to the new value.
         * <p>
         * This method delegates to {@link #setShowDiskLocation(boolean)}.
         * </p>
         *
         * @param showDiskLocation
         *            The new value for the if the disk location for each
         *            document should be returned.
         * @return This builder for chaining method calls.
         */
        public Builder showDiskLoc(final boolean showDiskLocation) {
            return setShowDiskLocation(showDiskLocation);
        }

        /**
         * Sets the value of the number of documents to skip before returning
         * the first document to the new value.
         * <p>
         * This method delegates to {@link #setNumberToSkip(int)}.
         * </p>
         *
         * @param numberToSkip
         *            The new value for the number of documents to skip before
         *            returning the first document.
         * @return This builder for chaining method calls.
         */
        public Builder skip(final int numberToSkip) {
            return setNumberToSkip(numberToSkip);
        }

        /**
         * Sets that the query should ensure that documents are only returned
         * once.
         * <p>
         * This method delegates to {@link #setSnapshot(boolean)
         * setSnapshot(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder snapshot() {
            return setSnapshot(true);
        }

        /**
         * Sets the value of snapshot to the new value. If set to true then use
         * snapshot mode to ensure document are only returned once.
         * <p>
         * This method delegates to {@link #setSnapshot(boolean)}.
         * </p>
         *
         *
         * @param snapshot
         *            The new value for the partial okay.
         * @return This builder for chaining method calls.
         */
        public Builder snapshot(final boolean snapshot) {
            return setSnapshot(snapshot);
        }

        /**
         * Sets the value of the fields to to sort matching documents by.
         * <p>
         * This method delegates to {@link #setSort(DocumentAssignable)}.
         * </p>
         *
         * @param sortFields
         *            The new value for the fields to sort matching documents
         *            by.
         * @return This builder for chaining method calls.
         */
        public Builder sort(final DocumentAssignable sortFields) {
            return setSort(sortFields);
        }

        /**
         * Sets the value of the fields to to sort matching documents by.
         * <p>
         * This method delegates to {@link #setSort(IntegerElement...)}.
         * </p>
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
         * builder.sort( asc("f"), desc("g") );
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
        public Builder sort(final IntegerElement... sortFields) {
            return setSort(sortFields);
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
