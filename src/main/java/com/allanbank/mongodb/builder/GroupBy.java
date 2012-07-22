/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.allanbank.mongodb.bson.Document;

/**
 * Group provides a container for all of the options to a <tt>group</tt>
 * command. A {@link Builder} is provided to assist in creating a
 * {@link GroupBy}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GroupBy {

    /** The finalizer function to run for each group. */
    private final String myFinalizeFunction;

    /** The initial value for each group. */
    private final Document myInitialValue;

    /**
     * Function to return the key for a document. Used instead of the
     * {@link #getKeys} to dynamically determine the group for each document.
     */
    private final String myKeyFunction;

    /** The fields to group by. */
    private final Set<String> myKeys;

    /** The query to select the documents to run the group against. */
    private final Document myQuery;

    /**
     * The reduce function taking the previous value and the current value and
     * returning the new reduced value.
     */
    private final String myReduceFunction;

    /**
     * Creates a new Group.
     * 
     * @param builder
     *            The builder to copy the state from.
     * @throws AssertionError
     *             If neither the {@link #getKeys() keys} nor
     *             {@link #getKeyFunction() key function} have been set.
     */
    protected GroupBy(final Builder builder) throws AssertionError {

        assert (!builder.myKeys.isEmpty() || (builder.myKeyFunction != null)) : "Must specify either a set of keys for the group or a key function.";

        myKeys = Collections
                .unmodifiableSet(new HashSet<String>(builder.myKeys));
        myReduceFunction = builder.myReduceFunction;
        myInitialValue = builder.myInitialValue;
        myKeyFunction = builder.myKeyFunction;
        myQuery = builder.myQuery;
        myFinalizeFunction = builder.myFinalizeFunction;
    }

    /**
     * Returns the finalizer function to run for each group.
     * 
     * @return The finalizer function to run for each group.
     */
    public String getFinalizeFunction() {
        return myFinalizeFunction;
    }

    /**
     * Returns the initial value for each group.
     * 
     * @return The initial value for each group.
     */
    public Document getInitialValue() {
        return myInitialValue;
    }

    /**
     * Returns the function to return the key for a document. Used instead of
     * the {@link #getKeys} to dynamically determine the group for each
     * document.
     * 
     * @return The function to return the key for a document. Used instead of
     *         the {@link #getKeys} to dynamically determine the group for each
     *         document.
     */
    public String getKeyFunction() {
        return myKeyFunction;
    }

    /**
     * Returns the fields to group by.
     * 
     * @return The fields to group by.
     */
    public Set<String> getKeys() {
        return myKeys;
    }

    /**
     * Returns the query to select the documents to run the group against.
     * 
     * @return The query to select the documents to run the group against.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the reduce function taking the previous value and the current
     * value and returning the new reduced value.
     * 
     * @return The reduce function taking the previous value and the current
     *         value and returning the new reduced value.
     */
    public String getReduceFunction() {
        return myReduceFunction;
    }

    /**
     * Builder provides a builder for Group commands.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /** The finalizer function to run for each group. */
        protected String myFinalizeFunction;

        /** The initial value for the group. */
        protected Document myInitialValue;

        /**
         * Function to return the key for a document. Used instead of the
         * {@link #setKeys} to dynamically determine the group for each
         * document.
         */
        protected String myKeyFunction;

        /** The fields to group by. */
        protected final Set<String> myKeys;

        /** The query to select the documents to run the group against. */
        protected Document myQuery;

        /**
         * The reduce function taking the previous value and the current value
         * and returning the new reduced value.
         */
        protected String myReduceFunction;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            myKeys = new HashSet<String>();
        }

        /**
         * Creates a new {@link GroupBy} based on the current state of the
         * builder.
         * 
         * @return A new {@link GroupBy} based on the current state of the
         *         builder.
         * @throws AssertionError
         *             If neither the {@link #getKeys() keys} nor
         *             {@link #getKeyFunction() key function} have been set.
         */
        public GroupBy build() throws AssertionError {
            return new GroupBy(this);
        }

        /**
         * Sets the value of the finalizer function to run for each group.
         * 
         * @param finalizeFunction
         *            The new value for the finalizer function to run for each
         *            group.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setFinalizeFunction(final String finalizeFunction) {
            myFinalizeFunction = finalizeFunction;
            return this;
        }

        /**
         * Sets the value of the initial value for the group.
         * 
         * @param initialValue
         *            The new value for the initial value for the group.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setInitialValue(final Document initialValue) {
            myInitialValue = initialValue;
            return this;
        }

        /**
         * Sets the value of the function to return the key for a document. Used
         * instead of the {@link #setKeys} to dynamically determine the group
         * for each document.
         * 
         * @param keyFunction
         *            The new value for the function to return the key for a
         *            document. Used instead of the {@link #setKeys} to
         *            dynamically determine the group for each document.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setKeyFunction(final String keyFunction) {
            myKeyFunction = keyFunction;
            return this;
        }

        /**
         * Sets the fields to group by
         * 
         * @param keys
         *            The new fields to group by
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setKeys(final Set<String> keys) {
            myKeys.clear();
            if (keys != null) {
                myKeys.addAll(keys);
            }
            return this;
        }

        /**
         * Sets the value of the query to select the documents to run the group
         * against.
         * 
         * @param query
         *            The new value for the query to select the documents to run
         *            the group against.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setQuery(final Document query) {
            myQuery = query;
            return this;
        }

        /**
         * Sets the value of the reduce function taking the previous value and
         * the current value and returning the new reduced value.
         * 
         * @param reduceFunction
         *            The new value for the reduce function taking the previous
         *            value and the current value and returning the new reduced
         *            value.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setReduceFunction(final String reduceFunction) {
            myReduceFunction = reduceFunction;
            return this;
        }
    }
}
