/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;

/**
 * ParallelScan provides an immutable container for all of the options for a
 * {@code parallelCollectionScan}.
 * <p>
 * <b>Note</b>: The {@code parallelCollectionScan} does not work with sharded
 * clusters.
 * <p>
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/command/parallelCollectionScan/">parallelCollectionScan
 *      Command</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ParallelScan {
    /**
     * Helper for creating immutable {@link ParallelScan} queries.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The number of documents to be returned in each batch of results. */
        protected int myBatchSize;

        /** The preference for which servers to use to retrieve the results. */
        protected ReadPreference myReadPreference;

        /**
         * The desired number of iterators/cursors to create. The server may
         * return few than this number of iterators.
         * <p>
         * This value will be forced into the range [1, 10,000].
         * </p>
         */
        protected int myRequestedIteratorCount;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
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
         * Constructs a new {@link ParallelScan} object from the state of the
         * builder.
         *
         * @return The new {@link ParallelScan} object.
         */
        public ParallelScan build() {
            return new ParallelScan(this);
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
         * Sets the requested number of iterators/cursors to create to the new
         * value.
         * <p>
         * This value will be forced into the range [1, 10,000].
         * </p>
         * <p>
         * This method delegates to {@link #setRequestedIteratorCount(int)}.
         * </p>
         *
         * @param numberOfIterators
         *            The requested number of iterators/cursors to create.
         * @return This builder for chaining method calls.
         */
        public Builder requestedIteratorCount(final int numberOfIterators) {
            return setRequestedIteratorCount(numberOfIterators);
        }

        /**
         * Resets the builder back to its initial state for reuse.
         *
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myBatchSize = 0;
            myReadPreference = null;
            myRequestedIteratorCount = 1;

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
         * Sets the requested number of iterators/cursors to create to the new
         * value.
         * <p>
         * This value will be forced into the range [1, 10,000].
         * </p>
         *
         * @param numberOfIterators
         *            The requested number of iterators/cursors to create.
         * @return This builder for chaining method calls.
         */
        public Builder setRequestedIteratorCount(final int numberOfIterators) {
            myRequestedIteratorCount = Math.min(Math.max(numberOfIterators, 1),
                    10000);
            return this;
        }
    }

    /**
     * The first version of MongoDB to support the
     * {@code parallelCollectionScan} command.
     */
    public static final Version REQUIRED_VERSION = Version.parse("2.6.0");

    /**
     * Creates a new builder for a {@link ParallelScan}.
     *
     * @return The builder to construct a {@link ParallelScan}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The number of documents to be returned in each batch of results. */
    private final int myBatchSize;

    /** The preference for which servers to use to retrieve the results. */
    private final ReadPreference myReadPreference;

    /**
     * The requested number of iterators/cursors to create. The server may
     * return few than this number of iterators.
     * <p>
     * This value will be forced into the range [1, 10,000].
     * </p>
     */
    private final int myRequestedIteratorCount;

    /**
     * Creates a new ParallelScan.
     *
     * @param builder
     *            The builder to copy the query fields from.
     */
    protected ParallelScan(final Builder builder) {
        myBatchSize = builder.myBatchSize;
        myReadPreference = builder.myReadPreference;
        myRequestedIteratorCount = builder.myRequestedIteratorCount;
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
     * Returns the requested number of iterators/cursors to create. The server
     * may return few than this number of iterators.
     * <p>
     * This value will be forced into the range [1, 10,000].
     * </p>
     *
     * @return The requested number of iterators/cursors to create.
     */
    public int getRequestedIteratorCount() {
        return myRequestedIteratorCount;
    }
}
