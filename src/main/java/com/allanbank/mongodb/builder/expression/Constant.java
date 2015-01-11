/*
 * #%L
 * Constant.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder.expression;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.Element;

/**
 * Represents constant value in an expression.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class Constant
        implements Expression {

    /** The constant value. */
    private final Element myConstant;

    /**
     * Creates a new Constant.
     *
     * @param constant
     *            The constant element.
     */
    public Constant(final Element constant) {
        myConstant = constant;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the constant value's element with the specified
     * name.
     * </p>
     */
    @Override
    public Element toElement(final String name) {
        return myConstant.withName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the constant value.
     * </p>
     * <blockquote>
     *
     * <pre>
     * <code>
     * value
     * </code>
     * </pre>
     *
     * </blockquote>
     */
    @Override
    public String toString() {
        return myConstant.getValueAsString();
    }
}
