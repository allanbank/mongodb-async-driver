/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.util.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Group provides a container for all of the options to a <tt>group</tt>
 * command. A {@link Builder} is provided to assist in creating a
 * {@link GroupBy}.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GroupBy {
    /**
     * The first version of MongoDB to support the {@code group} command with
     * the ability to limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Find.MAX_TIMEOUT_VERSION;

    /**
     * Creates a new builder for a {@link GroupBy}.
     * 
     * @return The builder to construct a {@link GroupBy}.
     */
    public static Builder builder() {
        return new Builder();
    }

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

    /** The maximum amount of time to allow the command to run. */
    private final long myMaximumTimeMilliseconds;

    /** The query to select the documents to run the group against. */
    private final Document myQuery;

    /** The read preference to use. */
    private final ReadPreference myReadPreference;

    /**
     * The reduce function taking the previous value and the current value and
     * returning the new reduced value.
     */
    private final String myReduceFunction;

    /**
     * Creates a new GroupBy.
     * 
     * @param builder
     *            The builder to copy the state from.
     * @throws IllegalArgumentException
     *             If neither the {@link #getKeys() keys} nor
     *             {@link #getKeyFunction() key function} have been set.
     */
    protected GroupBy(final Builder builder) throws IllegalArgumentException {
        assertThat(
                !builder.myKeys.isEmpty() || (builder.myKeyFunction != null),
                "Must specify either a set of keys for the groupBy or a key function.");

        myKeys = Collections
                .unmodifiableSet(new HashSet<String>(builder.myKeys));
        myReduceFunction = builder.myReduceFunction;
        myInitialValue = builder.myInitialValue;
        myKeyFunction = builder.myKeyFunction;
        myQuery = builder.myQuery;
        myFinalizeFunction = builder.myFinalizeFunction;
        myReadPreference = builder.myReadPreference;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
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
     * Returns the query to select the documents to run the group against.
     * 
     * @return The query to select the documents to run the group against.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the {@link ReadPreference} specifying which servers may be used
     * to execute the {@link GroupBy} command.
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
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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

        /** The maximum amount of time to allow the command to run. */
        protected long myMaximumTimeMilliseconds;

        /** The query to select the documents to run the group against. */
        protected Document myQuery;

        /** The read preference to use. */
        protected ReadPreference myReadPreference;

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

            reset();
        }

        /**
         * Creates a new {@link GroupBy} based on the current state of the
         * builder.
         * 
         * @return A new {@link GroupBy} based on the current state of the
         *         builder.
         * @throws IllegalArgumentException
         *             If neither the {@link #getKeys() keys} nor
         *             {@link #getKeyFunction() key function} have been set.
         */
        public GroupBy build() throws IllegalArgumentException {
            return new GroupBy(this);
        }

        /**
         * Sets the value of the finalizer function to run for each group.
         * <p>
         * This method delegates to {@link #setFinalizeFunction(String)}.
         * </p>
         * 
         * @param finalizeFunction
         *            The new value for the finalizer function to run for each
         *            group.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder finalize(final String finalizeFunction) {
            return setFinalizeFunction(finalizeFunction);
        }

        /**
         * Sets the value of the initial value for the group.
         * <p>
         * This method delegates to {@link #setInitialValue(DocumentAssignable)}
         * .
         * </p>
         * 
         * @param initialValue
         *            The new value for the initial value for the group.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder initialValue(final DocumentAssignable initialValue) {
            return setInitialValue(initialValue);
        }

        /**
         * Sets the value of the function to return the key for a document. Used
         * instead of the {@link #setKeys} to dynamically determine the group
         * for each document.
         * <p>
         * This method delegates to {@link #setKeyFunction(String)}.
         * </p>
         * 
         * @param keyFunction
         *            The new value for the function to return the key for a
         *            document. Used instead of the {@link #setKeys} to
         *            dynamically determine the group for each document.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder key(final String keyFunction) {
            return setKeyFunction(keyFunction);
        }

        /**
         * Sets the fields to group by
         * <p>
         * This method delegates to {@link #setKeys(Set)}.
         * </p>
         * 
         * @param keys
         *            The new fields to group by
         * @return This {@link Builder} for method call chaining.
         */
        public Builder keys(final Set<String> keys) {
            return setKeys(keys);
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
         * Sets the value of the query to select the documents to run the group
         * against.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}.
         * </p>
         * 
         * @param query
         *            The new value for the query to select the documents to run
         *            the group against.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link GroupBy} command.
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
         * Sets the value of the reduce function taking the previous value and
         * the current value and returning the new reduced value.
         * <p>
         * This method delegates to {@link #setReduceFunction(String)}.
         * </p>
         * 
         * @param reduceFunction
         *            The new value for the reduce function taking the previous
         *            value and the current value and returning the new reduced
         *            value.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder reduce(final String reduceFunction) {
            return setReduceFunction(reduceFunction);
        }

        /**
         * Resets the builder back to its initial state.
         * 
         * @return This {@link Builder} for method call chaining.
         */
        public Builder reset() {
            myFinalizeFunction = null;
            myInitialValue = null;
            myKeyFunction = null;
            myKeys.clear();
            myQuery = null;
            myReadPreference = null;
            myReduceFunction = null;
            myMaximumTimeMilliseconds = 0;

            return this;
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
        public Builder setInitialValue(final DocumentAssignable initialValue) {
            myInitialValue = initialValue.asDocument();
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
         * Sets the value of the query to select the documents to run the group
         * against.
         * 
         * @param query
         *            The new value for the query to select the documents to run
         *            the group against.
         * @return This {@link Builder} for method call chaining.
         */
        public Builder setQuery(final DocumentAssignable query) {
            myQuery = query.asDocument();
            return this;
        }

        /**
         * Sets the {@link ReadPreference} specifying which servers may be used
         * to execute the {@link GroupBy} command.
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
