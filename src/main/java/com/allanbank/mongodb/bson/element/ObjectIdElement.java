/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON Object Id.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ObjectIdElement extends AbstractElement {

    /** The BSON type for a Object Id. */
    public static final String DEFAULT_NAME = "_id";

    /** The BSON type for a Object Id. */
    public static final ElementType TYPE = ElementType.OBJECT_ID;

    /** The BSON Object id. */
    private final ObjectId myId;

    /**
     * Constructs a new {@link ObjectIdElement}.
     * 
     * @param name
     *            The name for the BSON Object Id.
     * @param id
     *            The object id.
     */
    public ObjectIdElement(final String name, final ObjectId id) {
        super(TYPE, name);
        myId = id;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitObjectId} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitObjectId(getName(), myId);
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
            final ObjectIdElement other = (ObjectIdElement) object;

            result = myId.equals(other.myId) && super.equals(object);
        }
        return result;
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
        result = (31 * result) + myId.hashCode();
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

        builder.append(myId);

        builder.append(")");

        return builder.toString();
    }
}
