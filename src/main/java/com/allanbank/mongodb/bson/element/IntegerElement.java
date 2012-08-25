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
 * A wrapper for a BSON (signed 32-bit) integer.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IntegerElement extends AbstractElement implements NumericElement {

    /** The BSON type for a integer. */
    public static final ElementType TYPE = ElementType.INTEGER;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 3738845320555958508L;

    /** The BSON integer value. */
    private final int myValue;

    /**
     * Constructs a new {@link IntegerElement}.
     * 
     * @param name
     *            The name for the BSON integer.
     * @param value
     *            The BSON integer value.
     */
    public IntegerElement(final String name, final int value) {
        super(name);

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
     * Returns a new {@link IntegerElement}.
     * </p>
     */
    @Override
    public IntegerElement withName(final String name) {
        return new IntegerElement(name, myValue);
    }
}
