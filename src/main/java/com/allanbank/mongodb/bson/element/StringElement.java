/*
 * #%L
 * StringElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON string.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringElement extends AbstractElement {

    /** The BSON type for a string. */
    public static final ElementType TYPE = ElementType.STRING;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 2279503881395893379L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     * 
     * @param name
     *            The name for the element.
     * @param value
     *            The BSON string value.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name, final String value) {
        long result = 7; // type (1) + name null byte (1) +
        // value length (4) + value null byte (1)
        result += StringEncoder.utf8Size(name);
        result += StringEncoder.utf8Size(value);

        return result;
    }

    /** The BSON string value. */
    private final String myValue;

    /**
     * Constructs a new {@link StringElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param value
     *            The BSON string value.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public StringElement(final String name, final String value) {
        this(name, value, computeSize(name, value));
    }

    /**
     * Constructs a new {@link StringElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param value
     *            The BSON string value.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link StringElement#StringElement(String, String)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public StringElement(final String name, final String value, final long size) {
        super(name, size);

        assertNotNull(value, "String element's value cannot be null.");

        myValue = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitString} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitString(getName(), getValue());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the string values if the base class comparison is
     * equals.
     * </p>
     * <p>
     * Note that for MongoDB {@link SymbolElement} and {@link StringElement}
     * will return equal based on the type. Care is taken here to make sure that
     * the values return the same value regardless of comparison order.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            // Might be a StringElement or SymbolElement.
            final ElementType otherType = otherElement.getType();

            if (otherType == ElementType.SYMBOL) {
                result = myValue.compareTo(((SymbolElement) otherElement)
                        .getSymbol());
            }
            else {
                result = myValue.compareTo(((StringElement) otherElement)
                        .getValue());
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
            final StringElement other = (StringElement) object;

            result = super.equals(object)
                    && nullSafeEquals(myValue, other.myValue);
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
     * Returns the BSON string value.
     * 
     * @return The BSON string value.
     */
    public String getValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the {@link String} value.
     * </p>
     */
    @Override
    public String getValueAsObject() {
        return getValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the {@link String} value.
     * </p>
     */
    @Override
    public String getValueAsString() {
        return getValue();
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
        result = (31 * result) + ((myValue != null) ? myValue.hashCode() : 3);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link StringElement}.
     * </p>
     */
    @Override
    public StringElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new StringElement(name, myValue);
    }
}
