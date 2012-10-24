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
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DoubleElement extends AbstractElement implements NumericElement {

    /** The BSON type for a double. */
    public static final ElementType TYPE = ElementType.DOUBLE;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -7373717629447356968L;

    /** The BSON double value. */
    private final double myValue;

    /**
     * Constructs a new {@link DoubleElement}.
     * 
     * @param name
     *            The name for the BSON double.
     * @param value
     *            The BSON double value.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DoubleElement(final String name, final double value) {
        super(name);

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
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
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
     * {@inheritDoc}
     * <p>
     * Returns a new {@link DoubleElement}.
     * </p>
     */
    @Override
    public DoubleElement withName(final String name) {
        return new DoubleElement(name, myValue);
    }
}
