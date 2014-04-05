/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * AggregationGroupField holds the information for an aggregation field in an
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
public class AggregationGroupField {

    /**
     * Builder provides the ability to construct a {@link AggregationGroupField}
     * .
     * <p>
     * This {@link Builder} does not support a <tt>build()</tt> method or
     * support chaining of method calls. Instead each builder method returns a
     * {@link AggregationGroupField}. This is to more closely match the
     * semantics of the group operator which cannot build complex structures for
     * each field.
     * </p>
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class Builder {

        /**
         * The name of the field to set with the result of the
         * aggregation/group.
         */
        private final String myFieldName;

        /**
         * Creates a new Builder.
         *
         * @param fieldName
         *            The name of the field to set with the result of the
         *            aggregation/group.
         */
        public Builder(final String fieldName) {
            myFieldName = fieldName;
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$addToSet</tt> operator to accumulate the unique values of the
         * <tt>fieldRef</tt>.
         * <p>
         * This is an alias for {@link #uniqueValuesOf(String)}.
         * </p>
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_addToSet">MongoDB
         *      Aggregate Command $group Operator - $addToSet</a>
         */
        public AggregationGroupField addToSet(final String fieldRef) {
            return uniqueValuesOf(fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$push</tt> operator to accumulate all of the values seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_push">MongoDB
         *      Aggregate Command $group Operator - $push</a>
         */
        public AggregationGroupField all(final String fieldRef) {
            return as("$push", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * custom operator to aggregate all of the values seen from the
         * <tt>fieldRef</tt>.
         *
         * @param operator
         *            The operator to use.
         * @param value
         *            The value to use in the aggregation.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
         *      Aggregate Command $group Operator</a>
         */
        public AggregationGroupField as(final String operator, final int value) {
            return new AggregationGroupField(myFieldName, operator, value);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * custom operator to aggregate all of the values seen from the
         * <tt>fieldRef</tt>.
         *
         * @param operator
         *            The operator to use.
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
         *      Aggregate Command $group Operator</a>
         */
        public AggregationGroupField as(final String operator,
                final String fieldRef) {
            return new AggregationGroupField(myFieldName, operator, fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$avg</tt> operator to compute the average value seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_avg">MongoDB
         *      Aggregate Command $group Operator - $avg</a>
         */
        public AggregationGroupField average(final String fieldRef) {
            return as("$avg", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$sum</tt> operator to count all of the input documents.
         *
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_sum">MongoDB
         *      Aggregate Command $group Operator - $sum</a>
         */
        public AggregationGroupField count() {
            return as("$sum", 1);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$first</tt> operator to use the first value seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_first">MongoDB
         *      Aggregate Command $group Operator - $first</a>
         */
        public AggregationGroupField first(final String fieldRef) {
            return as("$first", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$last</tt> operator to use the last value seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_last">MongoDB
         *      Aggregate Command $group Operator - $last</a>
         */
        public AggregationGroupField last(final String fieldRef) {
            return as("$last", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$max</tt> operator to use the maximum value seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_max">MongoDB
         *      Aggregate Command $group Operator - $max</a>
         */
        public AggregationGroupField maximum(final String fieldRef) {
            return as("$max", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$min</tt> operator to use the minimum value seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_min">MongoDB
         *      Aggregate Command $group Operator - $min</a>
         */
        public AggregationGroupField minimum(final String fieldRef) {
            return as("$min", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$push</tt> operator to accumulate all of the values seen from the
         * <tt>fieldRef</tt>.
         * <p>
         * This is an alias for {@link #all(String)}.
         * </p>
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_push">MongoDB
         *      Aggregate Command $group Operator - $push</a>
         */
        public AggregationGroupField push(final String fieldRef) {
            return all(fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$sum</tt> operator to sum all of the values seen from the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_sum">MongoDB
         *      Aggregate Command $group Operator - $sum</a>
         */
        public AggregationGroupField sum(final String fieldRef) {
            return as("$sum", fieldRef);
        }

        /**
         * Constructs a new {@link AggregationGroupField} object that uses the
         * <tt>$addToSet</tt> operator to accumulate the unique values of the
         * <tt>fieldRef</tt>.
         *
         * @param fieldRef
         *            The field reference to use. If the <tt>fieldRef</tt> does
         *            not start with a '$' then one will be added.
         * @return The new {@link AggregationGroupField} object.
         * @see <a href=
         *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_addToSet">MongoDB
         *      Aggregate Command $group Operator - $addToSet</a>
         */
        public AggregationGroupField uniqueValuesOf(final String fieldRef) {
            return as("$addToSet", fieldRef);
        }
    }

    /**
     * Helper method for creating {@link AggregationGroupField.Builder}s using
     * with a specific fieldName.
     *
     * @param fieldName
     *            The name of the output document field to be set.
     * @return The builder for constructing the {@link AggregationGroupField}.
     * @see <a href=
     *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_group">MongoDB
     *      Aggregate Command $group Operator</a>
     */
    public static AggregationGroupField.Builder set(final String fieldName) {
        return new AggregationGroupField.Builder(fieldName);
    }

    /** The element for the group operator's field. */
    private final Element myElement;

    /**
     * Creates a new AggregationGroupField.
     *
     * @param name
     *            The name of the output document field to be set.
     * @param operator
     *            The operator to use to perform the aggregation.
     * @param value
     *            The value to supply the operator.
     */
    protected AggregationGroupField(final String name, final String operator,
            final int value) {
        myElement = new DocumentElement(name, new IntegerElement(operator,
                value));
    }

    /**
     * Creates a new AggregationGroupField.
     *
     * @param name
     *            The name of the output document field to be set.
     * @param operator
     *            The operator to use to perform the aggregation.
     * @param fieldRef
     *            The field reference to use. If the <tt>fieldRef</tt> does not
     *            start with a '$' then one will be added.
     */
    protected AggregationGroupField(final String name, final String operator,
            final String fieldRef) {
        String normailzedFieldRef = fieldRef;
        if (!fieldRef.startsWith("$")) {
            normailzedFieldRef = "$" + fieldRef;
        }

        myElement = new DocumentElement(name, new StringElement(operator,
                normailzedFieldRef));
    }

    /**
     * Returns the element for the group operator's field.
     *
     * @return The element for the group operator's field.
     */
    public Element toElement() {
        return myElement;
    }
}
