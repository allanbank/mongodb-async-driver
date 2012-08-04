/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON string.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringElement extends AbstractElement {

    /** The BSON type for a string. */
    public static final ElementType TYPE = ElementType.STRING;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 2279503881395893379L;

    /** The BSON string value. */
    private final String myValue;

    /**
     * Constructs a new {@link StringElement}.
     * 
     * @param name
     *            The name for the BSON string.
     * @param value
     *            The BSON string value.
     */
    public StringElement(final String name, final String value) {
        super(TYPE, name);

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
     * Returns the BSON string value.
     * 
     * @return The BSON string value.
     */
    public String getValue() {
        return myValue;
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
     * String form of the object.
     * 
     * @return A human readable form of the object.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append('"');
        builder.append(getName());
        builder.append("\" : \"");
        builder.append(myValue);
        builder.append("\"");

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link StringElement}.
     * </p>
     */
    @Override
    public StringElement withName(final String name) {
        return new StringElement(name, myValue);
    }
}
