/*
 * #%L
 * MaxKeyElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.element;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON maximum key element.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class MaxKeyElement
        extends AbstractElement {

    /**
     * The {@link MaxKeyElement}'s class to avoid the
     * {@link Class#forName(String) Class.forName(...)} overhead.
     */
    public static final Class<MaxKeyElement> MAX_KEY_CLASS = MaxKeyElement.class;

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.MAX_KEY;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7706652376786415426L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     *
     * @param name
     *            The name for the element.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name) {
        long result = 2; // type (1) + name null byte (1).
        result += StringEncoder.utf8Size(name);

        return result;
    }

    /**
     * Constructs a new {@link MaxKeyElement}.
     *
     * @param name
     *            The name for the BSON maximum key.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MaxKeyElement(final String name) {
        this(name, computeSize(name));
    }

    /**
     * Constructs a new {@link MaxKeyElement}.
     *
     * @param name
     *            The name for the BSON maximum key.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the {@link MaxKeyElement#MaxKeyElement(String)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MaxKeyElement(final String name, final long size) {
        super(name, size);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitMaxKey} method.
     *
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitMaxKey(getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@link Double} with the value {@link Double#POSITIVE_INFINITY}.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. Double with the {@link Double#POSITIVE_INFINITY} value is
     * created instead.
     * </p>
     */
    @Override
    public Double getValueAsObject() {
        return Double.valueOf(Double.POSITIVE_INFINITY);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link MaxKeyElement}.
     * </p>
     */
    @Override
    public MaxKeyElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new MaxKeyElement(name);
    }
}
