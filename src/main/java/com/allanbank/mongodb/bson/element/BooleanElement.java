/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON boolean.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BooleanElement extends AbstractElement {

    /** The boolean value */
    private final boolean myValue;

    /**
     * Constructs a new {@link BooleanElement}.
     * 
     * @param name
     *            The name for the BSON boolean.
     * @param value
     *            The BSON boolean value.
     */
    public BooleanElement(final String name, final boolean value) {
        super(ElementType.BOOLEAN, name);
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
        return super.equals(object);
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
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return super.hashCode() + (myValue ? 31 : 11);
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
        builder.append("\" : ");
        builder.append(getValue());

        return builder.toString();
    }
}
