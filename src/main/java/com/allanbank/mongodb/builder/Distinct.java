/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Distinct {

    /** The name of the key to collect distinct values for. */
    private final String myKey;

    /** The query to select document to perform a distinct query across. */
    private final Document myQuery;

    /** The read preference to use. */
    private final ReadPreference myReadPreference;

    /**
     * Creates a new Distinct.
     * 
     * @param builder
     *            The builder to copy the state from.
     * @throws AssertionError
     *             If neither the {@link #getKey() key} is <code>null</code> or
     *             empty.
     */
    protected Distinct(final Builder builder) {
        assert ((builder.myKey != null) && !builder.myKey.isEmpty()) : "The distinct's command key cannot be null or empty.";

        myKey = builder.myKey;
        myQuery = builder.myQuery;
        myReadPreference = builder.myReadPreference;
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
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The name of the key to collect distinct values for. */
        protected String myKey;

        /** The query to select document to perform a distinct query across. */
        protected Document myQuery;

        /** The read preference to use. */
        protected ReadPreference myReadPreference;

        /**
         * Creates a new {@link GroupBy} based on the current state of the
         * builder.
         * 
         * @return A new {@link GroupBy} based on the current state of the
         *         builder.
         * @throws AssertionError
         *             If neither the {@link #setKey key} is <code>null</code>
         *             or empty.
         */
        public Distinct build() throws AssertionError {
            return new Distinct(this);
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
