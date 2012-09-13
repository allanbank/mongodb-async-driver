/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.MongoCollection;
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
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FindAndModify {

    /** The subset of fields to retrieve from the matched document. */
    private final Document myFields;

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
        assert (builder.myQuery != null) : "The findAndModify's query document cannot be null or empty.";
        assert ((builder.myUpdate != null) || builder.myRemove) : "The findAndModify must have an update document or be a remove.";

        myQuery = builder.myQuery;
        myUpdate = builder.myUpdate;
        mySort = builder.mySort;
        myFields = builder.myFields;
        myUpsert = builder.myUpsert;
        myReturnNew = builder.myReturnNew;
        myRemove = builder.myRemove;
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
     * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {
        /** Retrieve a subset of fields from the matched document. */
        protected Document myFields = null;

        /** A query to locate the document to update. */
        protected Document myQuery = null;

        /** Set to a true to remove the object before returning */
        protected boolean myRemove = false;

        /**
         * Set to true if you want to return the modified object rather than the
         * original. Ignored for remove.
         */
        protected boolean myReturnNew = false;

        /**
         * If multiple docs match, choose the first one in the specified sort
         * order as the object to manipulate.
         */
        protected Document mySort = null;

        /** Updates to be applied to the document. */
        protected Document myUpdate = null;

        /** Create object if it doesn't exist. */
        protected boolean myUpsert = false;

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
    }
}
