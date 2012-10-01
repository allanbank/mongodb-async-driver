/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
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
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MongoTimestampElement(final String name, final long value) {
        super(name);

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
     * {@inheritDoc}
     * <p>
     * Overridden to compare the times if the base class comparison is equals.
     * </p>
     * <p>
     * Note that for MongoDB {@link MongoTimestampElement} and
     * {@link TimestampElement} will return equal based on the type. Care is
     * taken here to make sure that the values return the same value regardless
     * of comparison order.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            // Might be a MongoTimestampElement or TimestampElement.
            final ElementType otherType = otherElement.getType();

            if (otherType == ElementType.UTC_TIMESTAMP) {
                result = Long.compare(getTime(),
                        ((TimestampElement) otherElement).getTime());
            }
            else {
                result = Long.compare(getTime(),
                        ((MongoTimestampElement) otherElement).getTime());
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
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
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
