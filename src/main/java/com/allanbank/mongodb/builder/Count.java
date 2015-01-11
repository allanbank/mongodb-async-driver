/*
 * #%L
 * Count.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Count provides an immutable container for all of the options for a query to
 * count documents.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class Count {

    /** An (empty) query document to find all documents. */
    public static final Document ALL = MongoCollection.ALL;

    /**
     * The first version of MongoDB to support the {@code count} command with
     * the ability to limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Find.MAX_TIMEOUT_VERSION;

    /**
     * Creates a new builder for a {@link Count}.
     *
     * @return The builder to construct a {@link Count}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The maximum amount of time to allow the query to run. */
    private final long myMaximumTimeMilliseconds;

    /** The query document. */
    private final Document myQuery;

    /** The preference for which servers to use to retrieve the results. */
    private final ReadPreference myReadPreference;

    /**
     * Creates a new Count.
     *
     * @param builder
     *            The builder to copy the query fields from.
     */
    protected Count(final Builder builder) {
        myQuery = builder.myQuery;
        myReadPreference = builder.myReadPreference;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
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
     * Helper for creating immutable {@link Count} queries.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    @NotThreadSafe
    public static class Builder {

        /** The maximum amount of time to allow the query to run. */
        protected long myMaximumTimeMilliseconds;

        /** The query document. */
        protected Document myQuery;

        /** The preference for which servers to use to retrieve the results. */
        protected ReadPreference myReadPreference;

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
         * Constructs a new {@link Count} object from the state of the builder.
         *
         * @return The new {@link Count} object.
         */
        public Count build() {
            return new Count(this);
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
            myQuery = ALL;
            myReadPreference = null;
            myMaximumTimeMilliseconds = 0;

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

    }
}
