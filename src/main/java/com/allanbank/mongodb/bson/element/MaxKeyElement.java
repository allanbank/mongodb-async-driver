/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON maximum key element.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MaxKeyElement extends AbstractElement {

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.MAX_KEY;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7706652376786415426L;

    /**
     * Constructs a new {@link MaxKeyElement}.
     * 
     * @param name
     *            The name for the BSON maximum key.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MaxKeyElement(final String name) {
        super(name);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitMaxKey} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitMaxKey(getName());
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
     * Returns a new {@link MaxKeyElement}.
     * </p>
     */
    @Override
    public MaxKeyElement withName(final String name) {
        return new MaxKeyElement(name);
    }
}
