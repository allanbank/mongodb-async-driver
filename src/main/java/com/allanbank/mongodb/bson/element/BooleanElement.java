/*
 * #%L
 * BooleanElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * A wrapper for a BSON boolean.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class BooleanElement
        extends AbstractElement {

    /**
     * The {@link BooleanElement}'s class to avoid the
     * {@link Class#forName(String) Class.forName(...)} overhead.
     */
    public static final Class<BooleanElement> BOOLEAN_CLASS = BooleanElement.class;

    /** The BSON type for a Object Id. */
    public static final ElementType TYPE = ElementType.BOOLEAN;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -3534279865960686134L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     *
     * @param name
     *            The name for the element.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name) {
        long result = 3; // type (1) + name null byte (1) + value (1).
        result += StringEncoder.utf8Size(name);

        return result;
    }

    /** The boolean value */
    private final boolean myValue;

    /**
     * Constructs a new {@link BooleanElement}.
     *
     * @param name
     *            The name for the BSON boolean.
     * @param value
     *            The BSON boolean value.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public BooleanElement(final String name, final boolean value) {
        this(name, value, computeSize(name));
    }

    /**
     * Constructs a new {@link BooleanElement}.
     *
     * @param name
     *            The name for the BSON boolean.
     * @param value
     *            The BSON boolean value.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link BooleanElement#BooleanElement(String, boolean)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public BooleanElement(final String name, final boolean value,
            final long size) {
        super(name, size);
        myValue = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitBoolean} method.
     *
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitBoolean(getName(), getValue());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the values if the base class comparison is equals.
     * False is less than true.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final BooleanElement other = (BooleanElement) otherElement;

            final int value = myValue ? 1 : 0;
            final int otherValue = other.myValue ? 1 : 0;

            result = value - otherValue;
        }

        return result;
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     *
     * @param object
     *            The object to compare to.
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final BooleanElement other = (BooleanElement) object;

            result = super.equals(object) && (myValue == other.myValue);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * Returns the BSON boolean value.
     *
     * @return The BSON boolean value.
     */
    public boolean getValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@link Boolean}.
     * </p>
     */
    @Override
    public Boolean getValueAsObject() {
        return Boolean.valueOf(getValue());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns "true" or "false".
     * </p>
     */
    @Override
    public String getValueAsString() {
        return Boolean.toString(myValue);
    }

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return super.hashCode() + (myValue ? 31 : 11);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link BooleanElement}.
     * </p>
     */
    @Override
    public BooleanElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new BooleanElement(name, myValue);
    }
}
