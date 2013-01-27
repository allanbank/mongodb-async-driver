/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON boolean.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BooleanElement extends AbstractElement {

    /** The BSON type for a Object Id. */
    public static final ElementType TYPE = ElementType.BOOLEAN;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -3534279865960686134L;

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
        super(name);
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
        return new BooleanElement(name, myValue);
    }
}
