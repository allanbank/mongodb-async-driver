/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON (signed 64-bit) Mongo timestamp as 4 byte increment and
 * 4 byte timestamp.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoTimestampElement extends AbstractElement {

    /** The BSON type for a long. */
    public static final ElementType TYPE = ElementType.MONGO_TIMESTAMP;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -402083578422199042L;

    /** The BSON timestamp value as 4 byte increment and 4 byte timestamp. */
    private final long myTimestamp;

    /**
     * Constructs a new {@link MongoTimestampElement}.
     * 
     * @param name
     *            The name for the BSON long.
     * @param value
     *            The BSON timestamp value as 4 byte increment and 4 byte
     *            timestamp.
     */
    public MongoTimestampElement(final String name, final long value) {
        super(TYPE, name);

        myTimestamp = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitMongoTimestamp}
     * method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitMongoTimestamp(getName(), getTime());
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
            final MongoTimestampElement other = (MongoTimestampElement) object;

            result = super.equals(object) && (myTimestamp == other.myTimestamp);
        }
        return result;
    }

    /**
     * Returns the BSON Mongo timestamp value as 4 byte increment and 4 byte
     * timestamp.
     * 
     * @return The BSON Mongo timestamp value as 4 byte increment and 4 byte
     *         timestamp.
     */
    public long getTime() {
        return myTimestamp;
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
        result = (31 * result) + (int) ((myTimestamp >> 32) & 0xFFFFFFFF);
        result = (31 * result) + (int) (myTimestamp & 0xFFFFFFFF);
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
        builder.append(myTimestamp);

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link MongoTimestampElement}.
     * </p>
     */
    @Override
    public MongoTimestampElement withName(final String name) {
        return new MongoTimestampElement(name, myTimestamp);
    }
}
