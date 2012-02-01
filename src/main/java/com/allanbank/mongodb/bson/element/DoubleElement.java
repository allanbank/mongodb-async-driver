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
 * A wrapper for a BSON double.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DoubleElement extends AbstractElement implements NumericElement {

    /** The BSON type for a double. */
    public static final ElementType TYPE = ElementType.DOUBLE;

    /** The BSON double value. */
    private final double myValue;

    /**
     * Constructs a new {@link DoubleElement}.
     * 
     * @param name
     *            The name for the BSON double.
     * @param value
     *            The BSON double value.
     */
    public DoubleElement(final String name, final double value) {
        super(TYPE, name);

        myValue = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitDouble} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitDouble(getName(), getValue());
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
            final DoubleElement other = (DoubleElement) object;

            result = super.equals(object) && (myValue == other.myValue);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the double value.
     * </p>
     */
    @Override
    public double getDoubleValue() {
        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to cast the double value to an integer.
     * </p>
     */
    @Override
    public int getIntValue() {
        return (int) myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to cast the double value to a long.
     * </p>
     */
    @Override
    public long getLongValue() {
        return (long) myValue;
    }

    /**
     * Returns the BSON double value.
     * 
     * @return The BSON double value.
     */
    public double getValue() {
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
        final long bits = Double.doubleToLongBits(myValue);

        result = (31 * result) + super.hashCode();
        result = (31 * result) + (int) (bits & 0xFFFFFFFF);
        result = (31 * result) + (int) ((bits >> 32) & 0xFFFFFFFF);
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
}
