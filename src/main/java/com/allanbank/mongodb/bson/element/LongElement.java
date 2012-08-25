/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON (signed 64-bit) integer or long.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LongElement extends AbstractElement implements NumericElement {

    /** The BSON type for a long. */
    public static final ElementType TYPE = ElementType.LONG;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2599658746763036474L;

    /** The BSON long value. */
    private final long myValue;

    /**
     * Constructs a new {@link LongElement}.
     * 
     * @param name
     *            The name for the BSON long.
     * @param value
     *            The BSON integer value.
     */
    public LongElement(final String name, final long value) {
        super(name);

        myValue = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitLong} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitLong(getName(), getValue());
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
            final LongElement other = (LongElement) object;

            result = super.equals(object) && (myValue == other.myValue);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the value as a double.
     * </p>
     */
    @Override
    public double getDoubleValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to cast the long value to an integer.
     * </p>
     */
    @Override
    public int getIntValue() {
        return (int) myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the value.
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
     * Returns the BSON long value.
     * 
     * @return The BSON long value.
     */
    public long getValue() {
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
        result = (31 * result) + (int) (myValue & 0xFFFFFFFF);
        result = (31 * result) + (int) ((myValue >> 32) & 0xFFFFFFFF);
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
        builder.append("\" : ");
        builder.append(myValue);

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link LongElement}.
     * </p>
     */
    @Override
    public LongElement withName(final String name) {
        return new LongElement(name, myValue);
    }
}
