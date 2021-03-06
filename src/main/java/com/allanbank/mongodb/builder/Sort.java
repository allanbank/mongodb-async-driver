/*
 * #%L
 * Sort.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * Provides the ability to easily specify the sort direction for an index or
 * sort specification.
 * <p>
 * MongoDB supports many different index types in addition to the simple
 * ascending and descending order. See the {@link Index} helper for more
 * information.
 * </p>
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public final class Sort {

    /** The value to indicate an ascending sort order. */
    public static final int ASCENDING = 1;

    /** The value to indicate an descending sort order. */
    public static final int DESCENDING = -1;

    /**
     * Creates an ascending order specification, e.g.,
     * <tt>{ &lt;field&gt; : 1 }</tt>.
     * <p>
     * This method is equivalent to {@link Index#asc(String)} method.
     * </p>
     *
     * @param field
     *            The field to create the ascending sort on.
     * @return The ascending sort specification.
     */
    public static IntegerElement asc(final String field) {
        return new IntegerElement(field, ASCENDING);
    }

    /**
     * Creates an descending order specification, e.g.,
     * <tt>{ &lt;field&gt; : -1 }</tt>.
     * <p>
     * This method is equivalent to {@link Index#desc(String)} method.
     * </p>
     *
     * @param field
     *            The field to create the descending sort on.
     * @return The descending sort specification.
     */
    public static IntegerElement desc(final String field) {
        return new IntegerElement(field, DESCENDING);
    }

    /**
     * Creates an 2D index specification, e.g.,
     * <tt>{ &lt;field&gt; : "2d" }</tt>.
     *
     * @param field
     *            The field to create the '2d' sort on.
     * @return The 2D index specification.
     * @deprecated Moved to the {@link Index} class as {@link Index#geo2d} to
     *             live with the other other index types. This method will be
     *             removed after the 1.3.0 release.
     */
    @Deprecated
    public static StringElement geo2d(final String field) {
        return Index.geo2d(field);
    }

    /**
     * Creates an natural ascending order sort specification, e.g.,
     * <tt>{ "$natural" : 1 }</tt>.
     *
     * @return The natural ascending sort specification.
     */
    public static IntegerElement natural() {
        return natural(ASCENDING);
    }

    /**
     * Creates an natural order sort specification with the specified
     * {@link #ASCENDING} or {@link #DESCENDING} order, e.g.,
     * <tt>{ "$natural" : &lt;direction&gt; }</tt>.
     *
     * @param direction
     *            The direction for the natural ordering, either
     *            {@link #ASCENDING} or {@link #DESCENDING}.
     * @return The descending sort specification.
     */
    public static IntegerElement natural(final int direction) {
        return new IntegerElement("$natural", direction);
    }

    /**
     * Creates a new Sort.
     */
    private Sort() {
        super();
    }
}
