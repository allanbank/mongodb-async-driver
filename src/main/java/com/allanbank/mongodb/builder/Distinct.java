/*
 * #%L
 * Distinct.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.util.Assertions.assertNotEmpty;

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
 * Distinct provides container for all of the options to a <tt>distinct</tt>
 * command. A {@link Builder} is provided to assist in creating a
 * {@link Distinct}.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class Distinct {
    /**
     * The first version of MongoDB to support the {@code distinct} command with
     * the ability to limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Find.MAX_TIMEOUT_VERSION;

    /**
     * Creates a new builder for a {@link Distinct}.
     *
     * @return The builder to construct a {@link Distinct}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The name of the key to collect distinct values for. */
    private final String myKey;

    /** The maximum amount of time to allow the command to run. */
    private final long myMaximumTimeMilliseconds;

    /** The query to select document to perform a distinct query across. */
    private final Document myQuery;

    /** The read preference to use. */
    private final ReadPreference myReadPreference;

    /**
     * Creates a new Distinct.
     *
     * @param builder
     *            The builder to copy the state from.
     * @throws IllegalArgumentException
     *             If neither the {@link #getKey() key} is <code>null</code> or
     *             empty.
     */
    protected Distinct(final Builder builder) {
        assertNotEmpty(builder.myKey,
                "The distinct's command key cannot be null or empty.");

        myKey = builder.myKey;
        myQuery = builder.myQuery;
        myReadPreference = builder.myReadPreference;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
    }

    /**
     * Returns the name of the key to collect distinct values for.
     *
     * @return The name of the key to collect distinct values for.
     */
    public String getKey() {
        return myKey;
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
     * Returns the query to select the documents to run the distinct against.
     *
     * @return The query to select the documents to run the distinct against.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the {@link ReadPreference} specifying which servers may be used
     * to execute the {@link Distinct} command.
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
     * Builder provides support for creating a {@link Distinct} object.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    @NotThreadSafe
    public static class Builder {

        /** The name of the key to collect distinct values for. */
        protected String myKey;

        /** The maximum amount of time to allow the command to run. */
        protected long myMaximumTimeMilliseconds;

        /** The query to select document to perform a distinct query across. */
        protected Document myQuery;

        /** The read preference to use. */
        protected ReadPreference myReadPreference;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Creates a new {@link GroupBy} based on the current state of the
         * builder.
         *
         * @return A new {@link GroupBy} based on the current state of the
         *         builder.
         * @throws IllegalArgumentException
         *             If neither the {@link #setKey key} is <code>null</code>
         *             or empty.
         */
        public Distinct build() throws IllegalArgumentException {
            return new Distinct(this);
        }

        /**
         * Sets the name of the key to collect distinct values for.
         * <p>
         * This method delegates to {@link #setKey(String)}.
         * </p>
         *
         * @param key
         *            The new name of the key to collect distinct values for.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder key(final String key) {
            return setKey(key);
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
         * Sets the value of the query to select the documents to run the
         * distinct against.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}.
         * </p>
         *
         * @param query
         *            The new value for the query to select the documents to run
         *            the distinct against.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link Distinct} command.
         * <p>
         * If not set or set to <code>null</code> then the
         * {@link MongoCollection} instance's {@link ReadPreference} will be
         * used.
         * </p>
         * <p>
         * This method delegates to {@link #setReadPreference(ReadPreference)}.
         * </p>
         *
         * @param readPreference
         *            The read preferences specifying which servers may be used.
         * @return This builder for chaining method calls.
         *
         * @see MongoCollection#getReadPreference()
         */
        public Builder readPreference(final ReadPreference readPreference) {
            return setReadPreference(readPreference);
        }

        /**
         * Resets the builder back to its initial state.
         *
         * @return This {@link Builder} for method call chaining.
         */
        public Builder reset() {
            myKey = null;
            myQuery = null;
            myReadPreference = null;
            myMaximumTimeMilliseconds = 0;

            return this;
        }

        /**
         * Sets the name of the key to collect distinct values for.
         *
         * @param key
         *            The new name of the key to collect distinct values for.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setKey(final String key) {
            myKey = key;
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
         * Sets the value of the query to select the documents to run the
         * distinct against.
         *
         * @param query
         *            The new value for the query to select the documents to run
         *            the distinct against.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setQuery(final DocumentAssignable query) {
            myQuery = query.asDocument();
            return this;
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link Distinct} command.
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
    }
}
