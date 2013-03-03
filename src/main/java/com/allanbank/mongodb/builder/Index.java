/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * Provides the ability to easily specify the index type of a field within an
 * index specification.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class Index {

    /**
     * Creates an ascending order specification, e.g.,
     * <tt>{ &lt;field&gt; : 1 }</tt>.
     * <p>
     * This method is equivalent to {@link Sort#asc(String)} method.
     * </p>
     * 
     * @param field
     *            The field to create the
     * @return The ascending sort specification.
     */
    public static IntegerElement asc(final String field) {
        return Sort.asc(field);
    }

    /**
     * Creates an descending order specification, e.g.,
     * <tt>{ &lt;field&gt; : -1 }</tt>.
     * <p>
     * This method is equivalent to the {@link Sort#desc(String)} method.
     * </p>
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
     * Creates an 2D Sphere index specification, e.g.,
     * <tt>{ &lt;field&gt; : "2dsphere" }</tt>.
     * 
     * @param field
     *            The field to create the
     * @return The 2D Sphere index specification.
     * @since MongoDB 2.4
     */
    public static StringElement geo2dSphere(final String field) {
        return new StringElement(field, "2dsphere");
    }

    /**
     * Creates an 'hashed' index specification, e.g.,
     * <tt>{ &lt;field&gt; : "hashed" }</tt>.
     * 
     * @param field
     *            The field to create the
     * @return The 'hashed' index specification.
     * @since MongoDB 2.4
     */
    public static StringElement hashed(final String field) {
        return new StringElement(field, "hashed");
    }

    /**
     * Creates an 'text' index specification, e.g.,
     * <tt>{ &lt;field&gt; : "text" }</tt>.
     * <p>
     * <b>Note:</b> 10gen considers text indexes to be an experimental feature
     * in the 2.4 release. Use with <b>extreme</b> caution. At a minimum make
     * sure you have read the <a
     * href="http://docs.mongodb.org/manual/release-notes/2.4/#text-indexes"
     * >MongoDB Text Index Documentation</a>.
     * </p>
     * 
     * @param field
     *            The field to create the
     * @return The 'text' index specification.
     * @since MongoDB 2.4
     * @see <a
     *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-indexes">MongoDB
     *      Text Index Documentation</a>
     */
    public static StringElement text(final String field) {
        return new StringElement(field, "text");
    }

    /**
     * Creates a new Sort.
     */
    private Index() {
        super();
    }
}
