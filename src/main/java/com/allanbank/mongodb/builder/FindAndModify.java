/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;
import static com.allanbank.mongodb.util.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.IntegerElement;

/**
 * Represents the state of a single {@link MongoCollection#findAndModify}
 * command. Objects of this class are created using the nested {@link Builder}.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FindAndModify {
    /** An (empty) query document to find all documents. */
    public static final Document ALL = MongoCollection.ALL;

    /**
     * The first version of MongoDB to support the {@code findAndModify} command
     * with the ability to limit the execution time on the server.
     */
    public static final Version MAX_TIMEOUT_VERSION = Find.MAX_TIMEOUT_VERSION;

    /** An (empty) update document to perform no actual modifications. */
    public static final Document NONE = MongoCollection.NONE;

    /**
     * Creates a new builder for a {@link FindAndModify}.
     *
     * @return The builder to construct a {@link FindAndModify}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** The subset of fields to retrieve from the matched document. */
    private final Document myFields;

    /** The maximum amount of time to allow the command to run. */
    private final long myMaximumTimeMilliseconds;

    /** The query to locate the document to update. */
    private final Document myQuery;

    /** Set to a true to remove the object before returning */
    private final boolean myRemove;

    /**
     * Set to true if you want to return the modified object rather than the
     * original. Ignored for remove.
     */
    private final boolean myReturnNew;

    /**
     * If multiple docs match, choose the first one in the specified sort order
     * as the object to manipulate.
     */
    private final Document mySort;

    /** The updates to be applied to the document. */
    private final Document myUpdate;

    /** If true create the document if it doesn't exist. */
    private final boolean myUpsert;

    /**
     * Create a new FindAndModify.
     *
     * @param builder
     *            The builder to copy from.
     */
    protected FindAndModify(final Builder builder) {
        assertNotNull(builder.myQuery,
                "The findAndModify's query document cannot be null or empty.");
        assertNotNull(builder.myQuery,
                "The findAndModify's query document cannot be null or empty.");
        assertThat((builder.myUpdate != null) || builder.myRemove,
                "The findAndModify must have an update document or be a remove.");

        myQuery = builder.myQuery;
        myUpdate = builder.myUpdate;
        mySort = builder.mySort;
        myFields = builder.myFields;
        myUpsert = builder.myUpsert;
        myReturnNew = builder.myReturnNew;
        myRemove = builder.myRemove;
        myMaximumTimeMilliseconds = builder.myMaximumTimeMilliseconds;
    }

    /**
     * Returns the subset of fields to retrieve from the matched document.
     *
     * @return The subset of fields to retrieve from the matched document.
     */
    public Document getFields() {
        return myFields;
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
     * Returns the query to locate the document to update.
     *
     * @return The query to locate the document to update.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns the sort to apply if multiple docs match, choose the first one as
     * the object to manipulate.
     *
     * @return The sort to apply if multiple docs match, choose the first one as
     *         the object to manipulate.
     */
    public Document getSort() {
        return mySort;
    }

    /**
     * Returns the updates to be applied to the document.
     *
     * @return The updates to be applied to the document.
     */
    public Document getUpdate() {
        return myUpdate;
    }

    /**
     * Returns true if the document should be removed.
     *
     * @return True if the document should be removed.
     */
    public boolean isRemove() {
        return myRemove;
    }

    /**
     * Returns true if the updated document should be returned instead of the
     * document before the update.
     *
     * @return True if the updated document should be returned instead of the
     *         document before the update.
     */
    public boolean isReturnNew() {
        return myReturnNew;
    }

    /**
     * Returns true to create the document if it doesn't exist.
     *
     * @return True to create the document if it doesn't exist.
     */
    public boolean isUpsert() {
        return myUpsert;
    }

    /**
     * Helper for creating immutable {@link FindAndModify} commands.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {
        /** Retrieve a subset of fields from the matched document. */
        protected Document myFields;

        /** The maximum amount of time to allow the command to run. */
        protected long myMaximumTimeMilliseconds;

        /** A query to locate the document to update. */
        protected Document myQuery;

        /** Set to a true to remove the object before returning */
        protected boolean myRemove;

        /**
         * Set to true if you want to return the modified object rather than the
         * original. Ignored for remove.
         */
        protected boolean myReturnNew;

        /**
         * If multiple docs match, choose the first one in the specified sort
         * order as the object to manipulate.
         */
        protected Document mySort;

        /** Updates to be applied to the document. */
        protected Document myUpdate;

        /** Create object if it doesn't exist. */
        protected boolean myUpsert;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Constructs a new {@link FindAndModify} object from the state of the
         * builder.
         *
         * @return The new {@link FindAndModify} object.
         */
        public FindAndModify build() {
            return new FindAndModify(this);
        }

        /**
         * Sets the subset of fields to retrieve from the matched document.
         * <p>
         * This method delegates to {@link #setFields(DocumentAssignable)}.
         * </p>
         *
         * @param fields
         *            The subset of fields to retrieve from the matched
         *            document.
         * @return This builder for chaining method calls.
         */
        public Builder fields(final DocumentAssignable fields) {
            return setFields(fields);
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
         * Sets the query to locate the document to update.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}.
         * </p>
         *
         * @param query
         *            The query to locate the document to update.
         * @return This builder for chaining method calls.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Sets to true if the document should be removed.
         * <p>
         * This method delegates to {@link #setRemove(boolean) setRemove(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder remove() {
            return setRemove(true);
        }

        /**
         * Sets to true if the document should be removed.
         * <p>
         * This method delegates to {@link #setRemove(boolean)}.
         * </p>
         *
         * @param remove
         *            True if the document should be removed.
         * @return This builder for chaining method calls.
         */
        public Builder remove(final boolean remove) {
            return setRemove(remove);
        }

        /**
         * Resets the builder back to its initial state.
         *
         * @return This {@link Builder} for method call chaining.
         */
        public Builder reset() {
            myFields = null;
            myQuery = null;
            myRemove = false;
            myReturnNew = false;
            mySort = null;
            myUpdate = null;
            myUpsert = false;
            myMaximumTimeMilliseconds = 0;

            return this;
        }

        /**
         * Sets to true if the updated document should be returned instead of
         * the document before the update.
         * <p>
         * This method delegates to {@link #setReturnNew(boolean)
         * setReturnNew(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder returnNew() {
            return setReturnNew(true);
        }

        /**
         * Sets to true if the updated document should be returned instead of
         * the document before the update.
         * <p>
         * This method delegates to {@link #setReturnNew(boolean)}.
         * </p>
         *
         * @param returnNew
         *            True if the updated document should be returned instead of
         *            the document before the update.
         * @return This builder for chaining method calls.
         */
        public Builder returnNew(final boolean returnNew) {
            return setReturnNew(returnNew);
        }

        /**
         * Sets the subset of fields to retrieve from the matched document.
         *
         * @param fields
         *            The subset of fields to retrieve from the matched
         *            document.
         * @return This builder for chaining method calls.
         */
        public Builder setFields(final DocumentAssignable fields) {
            myFields = fields.asDocument();
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
         * Sets the query to locate the document to update.
         *
         * @param query
         *            The query to locate the document to update.
         * @return This builder for chaining method calls.
         */
        public Builder setQuery(final DocumentAssignable query) {
            myQuery = query.asDocument();
            return this;
        }

        /**
         * Sets to true if the document should be removed.
         *
         * @param remove
         *            True if the document should be removed.
         * @return This builder for chaining method calls.
         */
        public Builder setRemove(final boolean remove) {
            myRemove = remove;
            return this;
        }

        /**
         * Sets to true if the updated document should be returned instead of
         * the document before the update.
         *
         * @param returnNew
         *            True if the updated document should be returned instead of
         *            the document before the update.
         * @return This builder for chaining method calls.
         */
        public Builder setReturnNew(final boolean returnNew) {
            myReturnNew = returnNew;
            return this;
        }

        /**
         * Sets the sort to apply if multiple docs match, choose the first one
         * as the object to manipulate.
         *
         * @param sort
         *            The sort to apply if multiple docs match, choose the first
         *            one as the object to manipulate.
         * @return This builder for chaining method calls.
         */
        public Builder setSort(final DocumentAssignable sort) {
            mySort = sort.asDocument();
            return this;
        }

        /**
         * Sets the sort to apply if multiple docs match, choose the first one
         * as the object to manipulate.
         * <p>
         * This method is intended to be used with the {@link Sort} class's
         * static methods: <blockquote>
         *
         * <pre>
         * <code>
         * import static {@link Sort#asc(String) com.allanbank.mongodb.builder.Sort.asc};
         * import static {@link Sort#desc(String) com.allanbank.mongodb.builder.Sort.desc};
         *
         * FindAndModify.Builder builder = new Find.Builder();
         *
         * builder.setSort( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         *
         * </blockquote>
         *
         * @param sortFields
         *            The sort to apply if multiple docs match, choose the first
         *            one as the object to manipulate.
         * @return This builder for chaining method calls.
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
         * Sets the updates to be applied to the document.
         *
         * @param update
         *            The updates to be applied to the document.
         * @return This builder for chaining method calls.
         */
        public Builder setUpdate(final DocumentAssignable update) {
            myUpdate = update.asDocument();
            return this;
        }

        /**
         * Sets to true to create the document if it doesn't exist.
         *
         * @param upsert
         *            True to create the document if it doesn't exist.
         * @return This builder for chaining method calls.
         */
        public Builder setUpsert(final boolean upsert) {
            myUpsert = upsert;
            return this;
        }

        /**
         * Sets the sort to apply if multiple docs match, choose the first one
         * as the object to manipulate.
         * <p>
         * This method delegates to {@link #setSort(DocumentAssignable)}.
         * </p>
         *
         * @param sort
         *            The sort to apply if multiple docs match, choose the first
         *            one as the object to manipulate.
         * @return This builder for chaining method calls.
         */
        public Builder sort(final DocumentAssignable sort) {
            return setSort(sort);
        }

        /**
         * Sets the sort to apply if multiple docs match, choose the first one
         * as the object to manipulate.
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
         * FindAndModify.Builder builder = new Find.Builder();
         *
         * builder.sort( asc("f"), desc("g") );
         * ...
         * </code>
         * </pre>
         *
         * </blockquote>
         *
         * @param sortFields
         *            The sort to apply if multiple docs match, choose the first
         *            one as the object to manipulate.
         * @return This builder for chaining method calls.
         */
        public Builder sort(final IntegerElement... sortFields) {
            return setSort(sortFields);
        }

        /**
         * Sets the updates to be applied to the document.
         * <p>
         * This method delegates to {@link #setUpdate(DocumentAssignable)}.
         * </p>
         *
         * @param update
         *            The updates to be applied to the document.
         * @return This builder for chaining method calls.
         */
        public Builder update(final DocumentAssignable update) {
            return setUpdate(update);
        }

        /**
         * Sets to true to create the document if it doesn't exist.
         * <p>
         * This method delegates to {@link #setUpsert(boolean) setUpsert(true)}.
         * </p>
         *
         * @return This builder for chaining method calls.
         */
        public Builder upsert() {
            return setUpsert(true);
        }

        /**
         * Sets to true to create the document if it doesn't exist.
         * <p>
         * This method delegates to {@link #setUpsert(boolean)}.
         * </p>
         *
         * @param upsert
         *            True to create the document if it doesn't exist.
         * @return This builder for chaining method calls.
         */
        public Builder upsert(final boolean upsert) {
            return setUpsert(upsert);
        }
    }
}
