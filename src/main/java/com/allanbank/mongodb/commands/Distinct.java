/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.commands;

import com.allanbank.mongodb.bson.Document;

/**
 * Distinct provides container for all of the options to a <tt>distinct</tt>
 * command. A {@link Builder} is provided to assist in creating a
 * {@link Distinct}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Distinct {

    /** The name of the key to collect distinct values for. */
    private final String myKey;

    /** The query to select document to perform a distinct query across. */
    private final Document myQuery;

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
     * Builder provides support for creating a {@link Distinct} object.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The name of the key to collect distinct values for. */
        protected String myKey;

        /** The query to select document to perform a distinct query across. */
        protected Document myQuery;

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
        public Builder setQuery(final Document query) {
            myQuery = query;
            return this;
        }

    }
}
