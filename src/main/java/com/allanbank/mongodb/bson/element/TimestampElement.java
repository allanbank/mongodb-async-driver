/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.Date;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON timestamp as the milliseconds since the epoch.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TimestampElement extends AbstractElement {

    /** The BSON type for a long. */
    public static final ElementType TYPE = ElementType.UTC_TIMESTAMP;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 949598909338399091L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     * 
     * @param name
     *            The name for the element.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name) {
        long result = 10; // type (1) + name null byte (1) + value (8).
        result += StringEncoder.utf8Size(name);

        return result;
    }

    /** The BSON timestamp value as the milliseconds since the epoch. */
    private final long myTimestamp;

    /**
     * Constructs a new {@link TimestampElement}.
     * 
     * @param name
     *            The name for the BSON long.
     * @param value
     *            The BSON timestamp value as the milliseconds since the epoch.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public TimestampElement(final String name, final long value) {
        this(name, value, computeSize(name));
    }

    /**
     * Constructs a new {@link TimestampElement}.
     * 
     * @param name
     *            The name for the BSON long.
     * @param value
     *            The BSON timestamp value as the milliseconds since the epoch.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link TimestampElement#TimestampElement(String, long)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public TimestampElement(final String name, final long value, final long size) {
        super(name, size);

        myTimestamp = value;
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitTimestamp} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitTimestamp(getName(), getTime());
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
                result = compare(getTime(),
                        ((TimestampElement) otherElement).getTime());
            }
            else {
                result = compare(getTime(),
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
            final TimestampElement other = (TimestampElement) object;

            result = super.equals(object) && (myTimestamp == other.myTimestamp);
        }
        return result;
    }

    /**
     * Returns the BSON timestamp value as the milliseconds since the epoch.
     * 
     * @return The BSON timestamp value as the milliseconds since the epoch.
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
     * {@inheritDoc}
     * <p>
     * Returns the {@link Date}.
     * </p>
     */
    @Override
    public Date getValueAsObject() {
        return new Date(getTime());
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
        result = (31 * result) + (int) (myTimestamp & 0xFFFFFFFF);
        result = (31 * result) + (int) ((myTimestamp >> 32) & 0xFFFFFFFF);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link TimestampElement}.
     * </p>
     */
    @Override
    public TimestampElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new TimestampElement(name, myTimestamp);
    }
}
