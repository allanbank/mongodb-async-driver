/*
 * #%L
 * Index.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class Index {

    /** The value to indicate an ascending index order. */
    public static final int ASCENDING = Sort.ASCENDING;

    /** The value to indicate an descending index order. */
    public static final int DESCENDING = Sort.DESCENDING;

    /** The value for the {@value} index. */
    public static final String GEO_2D_INDEX_NAME = "2d";

    /** The value for the {@value} index. */
    public static final String GEO_2DSPHERE_INDEX_NAME = "2dsphere";

    /** The value for the {@value} index. */
    public static final String GEO_HAYSTACK_INDEX_NAME = "geoHaystack";

    /** The value for the {@value} index. */
    public static final String HASHED_INDEX_NAME = "hashed";

    /** The value for the {@value} index. */
    public static final String TEXT_INDEX_NAME = "text";

    /**
     * Creates an ascending order specification, e.g.,
     * <tt>{ &lt;field&gt; : 1 }</tt>.
     * <p>
     * This method is equivalent to {@link Sort#asc(String)} method.
     * </p>
     * 
     * @param field
     *            The field to create the ascending index on.
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
     *            The field to create the descending index on.
     * @return The descending sort specification.
     */
    public static IntegerElement desc(final String field) {
        return Sort.desc(field);
    }

    /**
     * Creates an 2D index specification, e.g.,
     * <tt>{ &lt;field&gt; : "2d" }</tt>.
     * 
     * @param field
     *            The field to create the '2d' index on.
     * @return The 2D index specification.
     */
    public static StringElement geo2d(final String field) {
        return new StringElement(field, GEO_2D_INDEX_NAME);
    }

    /**
     * Creates an 2D Sphere index specification, e.g.,
     * <tt>{ &lt;field&gt; : "2dsphere" }</tt>.
     * 
     * @param field
     *            The field to create the '2dsphere' index on.
     * @return The 2D Sphere index specification.
     * @since MongoDB 2.4
     */
    public static StringElement geo2dSphere(final String field) {
        return new StringElement(field, GEO_2DSPHERE_INDEX_NAME);
    }

    /**
     * Creates a haystack index specification, e.g.,
     * <tt>{ &lt;field&gt; : "geoHaystack" }</tt>.
     * 
     * @param field
     *            The field to create the 'geoHaystack' index on.
     * @return The 2D Sphere index specification.
     * @see <a
     *      href="http://docs.mongodb.org/manual/applications/geohaystack/">Haystack
     *      Index Documentation</a>
     */
    public static StringElement geoHaystack(final String field) {
        return new StringElement(field, GEO_HAYSTACK_INDEX_NAME);
    }

    /**
     * Creates an 'hashed' index specification, e.g.,
     * <tt>{ &lt;field&gt; : "hashed" }</tt>.
     * 
     * @param field
     *            The field to create the 'hashed' index on.
     * @return The 'hashed' index specification.
     * @since MongoDB 2.4
     */
    public static StringElement hashed(final String field) {
        return new StringElement(field, HASHED_INDEX_NAME);
    }

    /**
     * Creates an 'text' index specification, e.g.,
     * <tt>{ &lt;field&gt; : "text" }</tt>.
     * <p>
     * <b>Note:</b> MongoDB Inc. considers text indexes to be an experimental
     * feature in the 2.4 release. Use with <b>extreme</b> caution. At a minimum
     * make sure you have read the <a
     * href="http://docs.mongodb.org/manual/release-notes/2.4/#text-indexes"
     * >MongoDB Text Index Documentation</a>.
     * </p>
     * 
     * @param field
     *            The field to create the 'text' index on.
     * @return The 'text' index specification.
     * @since MongoDB 2.4
     * @see <a
     *      href="http://docs.mongodb.org/manual/release-notes/2.4/#text-indexes">MongoDB
     *      Text Index Documentation</a>
     */
    public static StringElement text(final String field) {
        return new StringElement(field, TEXT_INDEX_NAME);
    }

    /**
     * Creates a new Sort.
     */
    private Index() {
        super();
    }
}
