/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON maximum key element.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
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
     */
    public MaxKeyElement(final String name) {
        super(TYPE, name);
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
        builder.append("\" : /* MAX_KEY */ ");
        builder.append(Long.MAX_VALUE);

        return builder.toString();
    }
}
