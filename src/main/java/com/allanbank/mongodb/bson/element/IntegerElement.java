/*
 * #%L
 * IntegerElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON (signed 32-bit) integer.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class IntegerElement
        extends AbstractElement
        implements NumericElement {

    /** The BSON type for a integer. */
    public static final ElementType TYPE = ElementType.INTEGER;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 3738845320555958508L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     *
     * @param name
     *            The name for the element.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name) {
        long result = 6; // type (1) + name null byte (1) + value (4).
        result += StringEncoder.utf8Size(name);

        return result;
    }

    /** The BSON integer value. */
    private final int myValue;

    /**
     * Constructs a new {@link IntegerElement}.
     *
     * @param name
     *            The name for the BSON integer.
     * @param value
     *            The BSON integer value.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public IntegerElement(final String name, final int value) {
        this(name, value, computeSize(name));
    }

    /**
     * Constructs a new {@link IntegerElement}.
     *
     * @param name
     *            The name for the BSON integer.
     * @param value
     *            The BSON integer value.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link IntegerElement#IntegerElement(String, int)} constructor
     *            instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public IntegerElement(final String name, final int value, final long size) {
        super(name, size);

        myValue = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitInteger} method.
     *
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitInteger(getName(), getValue());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the values if the base class comparison is equals.
     * </p>
     * <p>
     * Note that for MongoDB integers, longs, and doubles will return equal
     * based on the type. Care is taken here to make sure that double, integer,
     * and long values return the same value regardless of comparison order by
     * using the lowest resolution representation of the value.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            // Might be a IntegerElement, LongElement, or DoubleElement.
            final NumericElement other = (NumericElement) otherElement;
            final ElementType otherType = other.getType();

            // Integer is the lowest resolution.
            result = compare(getIntValue(), other.getIntValue());
            if (otherType == ElementType.INTEGER) {
                result = compare(getIntValue(), other.getIntValue());
            }
            else if (otherType == ElementType.LONG) {
                result = compare(getLongValue(), other.getLongValue());
            }
            else {
                result = Double.compare(getDoubleValue(),
                        other.getDoubleValue());

            }
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
            final IntegerElement other = (IntegerElement) object;

            result = super.equals(object) && (myValue == other.myValue);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the integer value as a double.
     * </p>
     */
    @Override
    public double getDoubleValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the integer value.
     * </p>
     */
    @Override
    public int getIntValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the integer value as a long.
     * </p>
     */
    @Override
    public long getLongValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * Returns the BSON integer value.
     *
     * @return The BSON integer value.
     */
    public int getValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@link Integer} with the value.
     * </p>
     */
    @Override
    public Integer getValueAsObject() {
        return Integer.valueOf(myValue);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the result of {@link Integer#toString(int)}.
     * </p>
     */
    @Override
    public String getValueAsString() {
        return Integer.toString(myValue);
    }

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result) + myValue;
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link IntegerElement}.
     * </p>
     */
    @Override
    public IntegerElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new IntegerElement(name, myValue);
    }
}
