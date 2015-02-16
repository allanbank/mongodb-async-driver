/*
 * #%L
 * NumericElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * NumericElement provides a common interface for all numeric {@link Element}s.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public interface NumericElement
        extends Element {

    /**
     * The {@link NumericElement}'s class to avoid the
     * {@link Class#forName(String) Class.forName(...)} overhead.
     */
    public static final Class<NumericElement> NUMERIC_CLASS = NumericElement.class;

    /**
     * Returns the value cast to a double.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.DoubleElement}. </em>
     *
     * @return The numeric value as a double.
     */
    public double getDoubleValue();

    /**
     * Returns the value cast to an integer.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.IntegerElement}. </em>
     *
     * @return The numeric value as a double.
     */
    public int getIntValue();

    /**
     * Returns the value cast to a long.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.LongElement}. </em>
     *
     * @return The numeric value as a double.
     */
    public long getLongValue();
}
