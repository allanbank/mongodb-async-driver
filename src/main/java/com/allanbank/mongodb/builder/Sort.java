/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * Provides the ability to easily specify the sort direction for an index or
 * sort specification.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class Sort {

    /**
     * Creates an ascending order specification, e.g.,
     * <tt>{ &lt;field&gt; : 1 }</tt>.
     * 
     * @param field
     *            The field to create the
     * @return The ascending sort specification.
     */
    public static IntegerElement asc(final String field) {
        return new IntegerElement(field, 1);
    }

    /**
     * Creates an descending order specification, e.g.,
     * <tt>{ &lt;field&gt; : -1 }</tt>.
     * 
     * @param field
     *            The field to create the
     * @return The descending sort specification.
     */
    public static IntegerElement desc(final String field) {
        return new IntegerElement(field, -1);
    }

    /**
     * Creates an 2D index specification, e.g.,
     * <tt>{ &lt;field&gt; : "2d" }</tt>.
     * 
     * @param field
     *            The field to create the
     * @return The 2D index specification.
     */
    public static StringElement geo2d(final String field) {
        return new StringElement(field, "2d");
    }

    /**
     * Creates a new Sort.
     */
    private Sort() {
        super();
    }
}
