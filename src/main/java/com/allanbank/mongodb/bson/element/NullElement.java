/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON null.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NullElement extends AbstractElement {

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.NULL;

    /**
     * Constructs a new {@link NullElement}.
     * 
     * @param name
     *            The name for the BSON null.
     */
    public NullElement(final String name) {
        super(TYPE, name);
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
        builder.append("\" : null");

        return builder.toString();
    }
}
