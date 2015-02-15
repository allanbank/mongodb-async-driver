/*
 * #%L
 * Aggregate.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2015 Allanbank Consulting, Inc.
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
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;

/**
 * ListIndexes provides support for creating a {@code listIndexes} command.
 * <p>
 * Instances of this class are constructed via the inner {@link Builder} class.
 * <p>
 * <p>
 * Use of the ListIndexes operation will gracefully degrade to the appropriate
 * query operation on a server before 2.7.7.
 * <p>
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/command/listIndexes/">listIndexes
 *      Command</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class ListIndexes {

    /**
     * The first version of MongoDB to support the {@code listIndexes} command.
     */
    public static final Version REQUIRED_VERSION = Version.parse("2.7.7");

    /**
     * Creates a new builder for a {@link ListIndexes}.
     *
     * @return The builder to construct a {@link ListIndexes}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The number of index documents to be returned in each batch of results.
     */
    private final int myBatchSize;

    /** The total number of index documents to be returned. */
    private final int myLimit;

    /** The maximum amount of time to allow the command to run. */
    private final long myMaximumTimeMilliseconds;

    /** The read preference to use. */
    private final ReadPreference myReadPreference;

    /**
     * Creates a new Aggregation.
     *
     * @param builder
     *            The builder for the Aggregation instance.
     */
    protected ListIndexes(final Builder builder) {
        myBatchSize = builder.myBatchSize;
        myLimit = builder.myLimit;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
        myReadPreference = builder.myReadPreference;
    }

    /**
     * Returns the number of documents to be returned in each batch of results
     * by the cursor.
     *
     * @return The number of documents to be returned in each batch of results
     *         by the cursor.
     */
    public int getBatchSize() {
        return myBatchSize;
    }

    /**
     * Returns the total number of index documents to be returned.
     *
     * @return The total number of index documents to be returned.
     */
    public int getLimit() {
        return myLimit;
    }

    /**
     * Returns the maximum amount of time to allow the command to run on the
     * Server before it is aborted.
     *
     * @return The maximum amount of time to allow the command to run on the
     *         Server before it is aborted.
     *
     * @since MongoDB 2.6
     */
    public long getMaximumTimeMilliseconds() {
        return myMaximumTimeMilliseconds;
    }

    /**
     * Returns the {@link ReadPreference} specifying which servers may be used
     * to execute the aggregation.
     * <p>
     * If <code>null</code> then the {@link MongoCollection} instance's
     * {@link ReadPreference} will be used.
     * </p>
     *
     * @return The read preference to use.
     *
     * @see MongoCollection#getReadPreference()
     */
    public ReadPreference getReadPreference() {
        return myReadPreference;
    }

    /**
     * Builder provides the ability to construct {@link ListIndexes}.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/reference/command/listIndexes/">listIndexes
     *      Command</a>
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    @NotThreadSafe
    public static class Builder {

        /** The number of documents to be returned in each batch of results. */
        protected int myBatchSize;

        /** The total number of documents to be returned. */
        protected int myLimit;

        /** The maximum amount of time to allow the command to run. */
        protected long myMaximumTimeMilliseconds;

        /** The read preference to use. */
        protected ReadPreference myReadPreference;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Sets the value of the number of index documents to be returned in
         * each batch.
         * <p>
         * This method delegates to {@link #setBatchSize(int)}.
         * </p>
         * <p>
         * This method also sets the builder to use a cursor to true.
         * </p>
         *
         * @param batchSize
         *            The new value for the number of index documents to be
         *            returned in each batch.
         * @return This builder for chaining method calls.
         */
        public Builder batchSize(final int batchSize) {
            return setBatchSize(batchSize);
        }

        /**
         * Constructs a new {@link ListIndexes} object from the state of the
         * builder.
         *
         * @return The new {@link ListIndexes} object.
         */
        public ListIndexes build() {
            return new ListIndexes(this);
        }

        /**
         * Sets the value of the total number of index documents to be returned.
         * <p>
         * This method delegates to {@link #setLimit(int)}.
         * </p>
         *
         * @param limit
         *            The new value for the total number of index documents to
         *            be returned.
         * @return This builder for chaining method calls.
         */
        public Builder limit(final int limit) {
            return setLimit(limit);
        }

        /**
         * Sets the maximum number of milliseconds to allow the command to run
         * before aborting the request on the server.
         * <p>
         * This method equivalent to {@link #setMaximumTimeMilliseconds(long)
         * setMaximumTimeMilliseconds(timeLimitUnits.toMillis(timeLimit)}.
         * </p>
         *
         * @param timeLimit
         *            The new maximum amount of time to allow the command to
         *            run.
         * @param timeLimitUnits
         *            The units for the maximum amount of time to allow the
         *            command to run.
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
         * Resets the builder back to its initial state.
         *
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myBatchSize = 0;
            myLimit = 0;
            myMaximumTimeMilliseconds = 0;
            myReadPreference = null;

            return this;
        }

        /**
         * Sets the value of the number of index documents to be returned in
         * each batch.
         *
         * @param batchSize
         *            The new value for the number of index documents to be
         *            returned in each batch.
         * @return This builder for chaining method calls.
         */
        public Builder setBatchSize(final int batchSize) {
            myBatchSize = batchSize;
            return this;
        }

        /**
         * Sets the value of the total number of index documents to be returned.
         *
         * @param limit
         *            The new value for the total number of index documents to
         *            be returned.
         * @return This builder for chaining method calls.
         */
        public Builder setLimit(final int limit) {
            myLimit = limit;
            return this;
        }

        /**
         * Sets the maximum number of milliseconds to allow the command to run
         * before aborting the request on the server.
         *
         * @param maximumTimeMilliseconds
         *            The new maximum number of milliseconds to allow the
         *            command to run.
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
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the aggregation.
         * <p>
         * If not set or set to <code>null</code> then the
         * {@link MongoCollection} instance's {@link ReadPreference} will be
         * used.
         * </p>
         *
         * @param readPreference
         *            The read preferences specifying which servers may be used.
         * @return This builder for chaining method calls.
         *
         * @see MongoCollection#getReadPreference()
         */
        public Builder setReadPreference(final ReadPreference readPreference) {
            myReadPreference = readPreference;
            return this;
        }

        /**
         * Return the JSON for the current pipeline that would be constructed by
         * the builder.
         */
        @Override
        public String toString() {
            final StringBuilder b = new StringBuilder();

            b.append("listIndexes[");
            b.append("batchSize=").append(myBatchSize);
            b.append(", limit=").append(myLimit);
            b.append(", maxTime=").append(myMaximumTimeMilliseconds);
            b.append(" ms, readPreference=").append(myReadPreference);
            b.append("]");

            return b.toString();
        }
    }
}
