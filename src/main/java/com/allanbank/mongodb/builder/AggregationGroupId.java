/*
 * #%L
 * AggregationGroupId.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.impl.AbstractBuilder;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * AggregationGroupId holds the information for the <tt>_id</tt> field in an
 * aggregate command's
 * {@link Aggregate.Builder#group(AggregationGroupId, AggregationGroupField...)
 * $group} pipeline operator.
 *
 * @see <a href=
 *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
 *      Aggregate Command $group Operator</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class AggregationGroupId {

    /**
     * Constructs a {@link AggregationGroupId} with a constant value.
     *
     * @param value
     *            The value of the _id.
     * @return The AggregationGroupId with a constant value.
     * @see <a href=
     *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
     *      Aggregate Command $group Operator</a>
     */
    public static AggregationGroupId constantId(final String value) {
        return new AggregationGroupId(value);
    }

    /**
     * Creates a builder to construct a complex _id value for the group.
     *
     * @return The builder for the aggregation $group's id.
     * @see <a href=
     *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
     *      Aggregate Command $group Operator</a>
     */
    public static AggregationGroupId.Builder id() {
        return new AggregationGroupId.Builder();
    }

    /**
     * Constructs a {@link AggregationGroupId} with a value from a single field
     * in the source documents.
     *
     * @param fieldRef
     *            The field name in the source documents to use in constructing
     *            the _id of the group'd documents. If the <tt>fieldRef</tt>
     *            does not start with a '$' then one will be added.
     * @return The AggregationGroupId with a single field value reference (which
     *         may resolve to a complex sub-document).
     * @see <a href=
     *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
     *      Aggregate Command $group Operator</a>
     */
    public static AggregationGroupId id(final String fieldRef) {
        if (!fieldRef.startsWith("$")) {
            return new AggregationGroupId("$" + fieldRef);
        }

        return new AggregationGroupId(fieldRef);
    }

    /** The id element. */
    private final Element myIdElement;

    /**
     * Creates a new AggregationGroupId.
     *
     * @param builder
     *            The builder containing the details of the id.
     */
    public AggregationGroupId(final DocumentAssignable builder) {
        myIdElement = new DocumentElement("_id", builder.asDocument());
    }

    /**
     * Creates a new AggregationGroupId.
     *
     * @param value
     *            The value for the simple group id.
     */
    public AggregationGroupId(final String value) {
        myIdElement = new StringElement("_id", value);
    }

    /**
     * Returns the element for the group operator's id.
     *
     * @return The element for the group operator's id.
     */
    public Element toElement() {
        return myIdElement;
    }

    /**
     * Builder provides the ability to construct a complex
     * {@link AggregationGroupId}.
     * <p>
     * This {@link Builder} also implements the
     * {@link com.allanbank.mongodb.bson.builder.DocumentBuilder} interface to
     * allow the construction of arbitrarily complex id documents.
     * </p>
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    @NotThreadSafe
    public static class Builder
            extends DocumentBuilderImpl {

        /**
         * Creates a new Builder.
         */
        public Builder() {
            super((AbstractBuilder) null);
        }

        /**
         * Adds a field reference to the id document. The output field name is
         * the same as the input field.
         * <p>
         * This method is equivalent to {@link #addField(String, String)
         * addField(&lt;fieldRef&gt;, &lt;fieldRef&gt;)} with appropriate
         * handling for the '$' prefix.
         * </p>
         *
         * @param fieldRef
         *            The dotted field path for the field to use. If the
         *            <tt>fieldRef</tt> does not start with a '$' then one will
         *            be added.
         * @return This builder for chaining method calls.
         */
        public Builder addField(final String fieldRef) {
            if (!fieldRef.startsWith("$")) {
                add(new StringElement(fieldRef, "$" + fieldRef));
            }
            else {
                add(new StringElement(fieldRef.substring(1), fieldRef));
            }
            return this;
        }

        /**
         * Adds a field reference to the id document.
         *
         * @param name
         *            The name of the field in the id document.
         * @param fieldRef
         *            The dotted field path for the field to use. If the
         *            <tt>fieldRef</tt> does not start with a '$' then one will
         *            be added.
         * @return This builder for chaining method calls.
         */
        public Builder addField(final String name, final String fieldRef) {
            if (!fieldRef.startsWith("$")) {
                add(new StringElement(name, "$" + fieldRef));
            }
            else {
                add(new StringElement(name, fieldRef));
            }
            return this;
        }

        /**
         * Constructs a new {@link AggregationGroupId} object from the state of
         * the builder.
         *
         * @return The new {@link AggregationGroupId} object.
         */
        public AggregationGroupId buildId() {
            return new AggregationGroupId(this);
        }
    }
}
