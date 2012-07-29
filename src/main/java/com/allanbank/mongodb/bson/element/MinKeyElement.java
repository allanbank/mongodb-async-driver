/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON minimum key element.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
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
     */
    public MinKeyElement(final String name) {
        super(TYPE, name);
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
        builder.append("\" : /* MIN_KEY */ ");
        builder.append(Long.MIN_VALUE);

        return builder.toString();
    }
}
