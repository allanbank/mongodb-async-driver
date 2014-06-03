/*
 * #%L
 * MongoTimestampElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON (signed 64-bit) Mongo timestamp as 4 byte increment and
 * 4 byte timestamp.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoTimestampElement extends AbstractElement {

    /** The BSON type for a long. */
    public static final ElementType TYPE = ElementType.MONGO_TIMESTAMP;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -402083578422199042L;

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
        this(name, value, computeSize(name));
    }

    /**
     * Constructs a new {@link MongoTimestampElement}.
     * 
     * @param name
     *            The name for the BSON long.
     * @param value
     *            The BSON timestamp value as 4 byte increment and 4 byte
     *            timestamp.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link MongoTimestampElement#MongoTimestampElement(String, long)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public MongoTimestampElement(final String name, final long value,
            final long size) {
        super(name, size);

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

            if (otherType == ElementType.MONGO_TIMESTAMP) {
                result = compare(getTime(),
                        ((MongoTimestampElement) otherElement).getTime());
            }
            else {
                result = compare(getTime(),
                        ((TimestampElement) otherElement).getTime());
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
     * {@inheritDoc}
     * <p>
     * Returns a {@link Long} with the value of the timestamp.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. Long with the value of the timestamp is created instead.
     * </p>
     */
    @Override
    public Long getValueAsObject() {
        return Long.valueOf(myTimestamp);
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
        if (getName().equals(name)) {
            return this;
        }
        return new MongoTimestampElement(name, myTimestamp);
    }
}
