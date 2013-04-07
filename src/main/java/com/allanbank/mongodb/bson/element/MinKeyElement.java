/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON minimum key element.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MinKeyElement extends AbstractElement {

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.MIN_KEY;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 5715544436237499313L;

    /**
     * Constructs a new {@link MinKeyElement}.
     * 
     * @param name
     *            The name for the BSON minimum key.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MinKeyElement(final String name) {
        super(name);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitMinKey} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitMinKey(getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@link Double} with the value {@link Double#NEGATIVE_INFINITY}.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. Double with the {@link Double#NEGATIVE_INFINITY} value is
     * created instead.
     * </p>
     */
    @Override
    public Double getValueAsObject() {
        return Double.valueOf(Double.NEGATIVE_INFINITY);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link MinKeyElement}.
     * </p>
     */
    @Override
    public MinKeyElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new MinKeyElement(name);
    }
}
