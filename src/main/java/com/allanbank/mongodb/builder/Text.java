/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.util.Assertions.assertNotEmpty;

import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Text provides a wrapper for a {@code text} command to query a collection with
 * a {@link Index#text(String) text index}.
 * <p>
 * The result of a {@code text} command is a document that looks like the
 * following:<blockquote>
 * 
 * <pre>
 * <code>
 * > db.collection.runCommand( { "text": "collection" , search: "coffee magic" } )
 * {
 *     "queryDebugString" : "coffe|magic||||||",
 *     "language" : "english",
 *     "results" : [
 *         {
 *             "score" : 2.25,
 *             "obj" : {
 *                 "_id" : ObjectId("51376ab8602c316554cfe248"),
 *                 "content" : "Coffee is full of magical powers."
 *             }
 *         },
 *         {
 *             "score" : 0.625,
 *             "obj" : {
 *                 "_id" : ObjectId("51376a80602c316554cfe246"),
 *                 "content" : "Now is the time to drink all of the coffee."
 *             }
 *         }
 *     ],
 *     "stats" : {
 *         "nscanned" : 3,
 *         "nscannedObjects" : 0,
 *         "n" : 2,
 *         "nfound" : 2,
 *         "timeMicros" : 97
 *     },
 *     "ok" : 1
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * The {@link TextResult} class wraps a single entry from the {@code results}
 * array.
 * </p>
 * 
 * @api.no <b>This class is NOT part of the Public API.</b> This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 *         This class <b>WILL</b>, eventually, be part of the driver's API.
 *         Until 10gen finalizes the text query interface we are keeping this
 *         class out of the Public API so we can track any changes more closely.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 * @see <a
 *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-queries">
 *      MongoDB Text Queries</a>
 * @since MongoDB 2.4
 */
public class Text {
    /**
     * The first version of MongoDB to support the {@code text} command with the
     * ability to limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Version.parse("2.5.4");

    /** The first version of MongoDB to support the text command. */
    public static final Version REQUIRED_VERSION = Version.parse("2.4.0");

    /**
     * Creates a new builder for a {@link Text} command.
     * 
     * @return The builder to construct a {@link Text} command.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The language to use when stemming the search terms. */
    private final String myLanguage;

    /** Maximum number of document to return. */
    private final int myLimit;

    /** The maximum amount of time to allow the command to run. */
    private final long myMaximumTimeMilliseconds;

    /** A standard MongoDB query document to limit the final results. */
    private final Document myQuery;

    /** The read preference to use. */
    private final ReadPreference myReadPreference;

    /** The fields to return from the query. */
    private final Document myReturnFields;

    /** The search terms. */
    private final String mySearchTerm;

    /**
     * Creates a new Text.
     * 
     * @param builder
     *            The builder containing the state of the text command.
     * @throws IllegalArgumentException
     *             On the search term not being set.
     */
    protected Text(final Builder builder) {
        assertNotEmpty(builder.mySearchTerm,
                "The search term for a 'text' command must be a non-empty string.");

        myLanguage = builder.myLanguage;
        myLimit = builder.myLimit;
        myQuery = builder.myQuery;
        myReadPreference = builder.myReadPreference;
        myReturnFields = builder.myReturnFields;
        mySearchTerm = builder.mySearchTerm;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
    }

    /**
     * Returns the language to use when stemming the search terms.
     * 
     * @return The language to use when stemming the search terms.
     */
    public String getLanguage() {
        return myLanguage;
    }

    /**
     * Returns the maximum number of document to return.
     * 
     * @return The maximum number of document to return.
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
     * Returns the query document to limit the final results.
     * 
     * @return The query document to limit the final results.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the {@link ReadPreference} specifying which servers may be used
     * to execute the {@link Text} command.
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
     * Returns the fields to return from the query.
     * 
     * @return The fields to return from the query.
     */
    public Document getReturnFields() {
        return myReturnFields;
    }

    /**
     * Returns the search terms.
     * 
     * @return The search terms.
     */
    public String getSearchTerm() {
        return mySearchTerm;
    }

    /**
     * Builder provides a builder for Text commands.
     * 
     * @api.no <b>This class is NOT part of the Public API.</b> This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver. This class <b>WILL</b>, eventually, be part of the
     *         driver's API. Until 10gen finalizes the text query interface we
     *         are keeping this class out of the Public API so we can track any
     *         changes more closely.
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {
        /** The language to use when stemming the search terms. */
        protected String myLanguage;

        /** Maximum number of document to return. */
        protected int myLimit;

        /** The maximum amount of time to allow the command to run. */
        protected long myMaximumTimeMilliseconds;

        /** A standard MongoDB query document to limit the final results. */
        protected Document myQuery;

        /** The read preference to use. */
        protected ReadPreference myReadPreference;

        /** The fields to return from the query. */
        protected Document myReturnFields;

        /** The search terms. */
        protected String mySearchTerm;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Creates a new {@link Text} based on the current state of the builder.
         * 
         * @return A new {@link Text} based on the current state of the builder.
         * @throws IllegalArgumentException
         *             On the search term not being set.
         */
        public Text build() {
            return new Text(this);
        }

        /**
         * Sets the language to use when stemming the search terms to the new
         * value.
         * <p>
         * This method delegates to {@link #setLanguage(String)}
         * </p>
         * 
         * @param language
         *            The new value for the language to use when stemming the
         *            search terms.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder language(final String language) {
            return setLanguage(language);
        }

        /**
         * Sets the maximum number of document to return to the new value.
         * <p>
         * This method delegates to {@link #setLimit(int)}
         * </p>
         * 
         * @param limit
         *            The new value for the maximum number of document to
         *            return.
         * @return This {@link Builder} for method call chaining.
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
         * Sets the standard MongoDB query document to limit the final results
         * to the new value.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}
         * </p>
         * 
         * @param query
         *            The new value for the standard MongoDB query document to
         *            limit the final results.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link Text} command.
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
            myLanguage = null;
            myLimit = 0;
            myMaximumTimeMilliseconds = 0;
            myQuery = null;
            myReadPreference = null;
            myReturnFields = null;
            mySearchTerm = null;

            return this;
        }

        /**
         * Sets the fields to return from the query to the new value.
         * <p>
         * This method delegates to {@link #setReturnFields(DocumentAssignable)}
         * </p>
         * 
         * @param returnFields
         *            The new value for the fields to return from the query.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder returnFields(final DocumentAssignable returnFields) {
            return setReturnFields(returnFields);
        }

        /**
         * Sets the search term to the new value.
         * <p>
         * This method delegates to {@link #setSearchTerm(String)}
         * </p>
         * 
         * @param searchTerm
         *            The new value for the search terms.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder searchTerm(final String searchTerm) {
            return setSearchTerm(searchTerm);
        }

        /**
         * Sets the language to use when stemming the search terms to the new
         * value.
         * 
         * @param language
         *            The new value for the language to use when stemming the
         *            search terms.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setLanguage(final String language) {
            myLanguage = language;
            return this;
        }

        /**
         * Sets the maximum number of document to return to the new value.
         * 
         * @param limit
         *            The new value for the maximum number of document to
         *            return.
         * @return This {@link Builder} for method call chaining.
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
         * Sets the standard MongoDB query document to limit the final results
         * to the new value.
         * 
         * @param query
         *            The new value for the standard MongoDB query document to
         *            limit the final results.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setQuery(final DocumentAssignable query) {
            if (query != null) {
                myQuery = query.asDocument();
            }
            else {
                myQuery = null;
            }
            return this;
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link Text} command.
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
         * Sets the fields to return from the query to the new value.
         * 
         * @param returnFields
         *            The new value for the fields to return from the query.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setReturnFields(final DocumentAssignable returnFields) {
            if (returnFields != null) {
                myReturnFields = returnFields.asDocument();
            }
            else {
                myReturnFields = null;
            }
            return this;
        }

        /**
         * Sets the search term to the new value.
         * 
         * @param searchTerm
         *            The new value for the search terms.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setSearchTerm(final String searchTerm) {
            mySearchTerm = searchTerm;
            return this;
        }
    }
}
