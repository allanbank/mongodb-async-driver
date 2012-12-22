/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON null.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NullElement extends AbstractElement {

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.NULL;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -4974513577366947524L;

    /**
     * Constructs a new {@link NullElement}.
     * 
     * @param name
     *            The name for the BSON null.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public NullElement(final String name) {
        super(name);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitNull} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitNull(getName());
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
     * Returns a new {@link NullElement}.
     * </p>
     */
    @Override
    public NullElement withName(final String name) {
        return new NullElement(name);
    }
}
