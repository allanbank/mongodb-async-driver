/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.element.IntegerElement;

/**
 * AggregationProjectFields holds the information for the fields to copy from
 * the input documents when using the aggregate command's
 * {@link Aggregate.Builder#project $project} pipeline operator.
 * 
 * @see <a href=
 *      "http://docs.mongodb.org/manual/reference/aggregation/#_S_project">MongoDB
 *      Aggregate Command $project Operator</a>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationProjectFields {
    /**
     * Helper method to create a {@link AggregationProjectFields} that
     * implicitly includes the _id field.
     * 
     * @param fields
     *            The fields to be included.
     * @return The {@link AggregationProjectFields}.
     */
    public static AggregationProjectFields include(final String... fields) {
        return new AggregationProjectFields(fields);

    }

    /**
     * Helper method to create a {@link AggregationProjectFields} that does not
     * include the _id field.
     * 
     * @param fields
     *            The fields to be included.
     * @return The {@link AggregationProjectFields}.
     */
    public static AggregationProjectFields includeWithoutId(
            final String... fields) {
        return new AggregationProjectFields(false, fields);
    }

    /**
     * The elements describing the fields to be included (and if the _id should
     * be excluded).
     */
    private final List<IntegerElement> myElements;

    /**
     * Creates a new AggregationProjectFields.
     * 
     * @param includeId
     *            If false then the _id field will be excluded from the fields
     *            in the results of the <tt>$project</tt> operation.
     * @param fields
     *            The fields to include in the results of the <tt>$project</tt>
     *            operation.
     */
    public AggregationProjectFields(final boolean includeId,
            final String... fields) {
        final List<IntegerElement> elements = new ArrayList<IntegerElement>(
                fields.length + 1);
        if (!includeId) {
            elements.add(new IntegerElement("_id", 0));
        }
        for (final String field : fields) {
            elements.add(new IntegerElement(field, 1));
        }

        myElements = Collections.unmodifiableList(elements);
    }

    /**
     * Creates a new AggregationProjectFields.
     * 
     * @param fields
     *            The fields to include in the results of the <tt>$project</tt>
     *            operation.
     */
    public AggregationProjectFields(final String... fields) {
        this(true, fields);
    }

    /**
     * Returns the elements describing the fields to be included (and if the _id
     * should be excluded).
     * 
     * @return The elements describing the fields to be included (and if the _id
     *         should be excluded).
     */
    public List<IntegerElement> toElements() {
        return myElements;
    }
}
