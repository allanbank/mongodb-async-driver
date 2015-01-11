/*
 * #%L
 * BinaryElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.StringEncoder;

/**
 * A wrapper for a BSON binary.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class BinaryElement
        extends AbstractElement {

    /** The sub type used when no sub type is specified. */
    public static final byte DEFAULT_SUB_TYPE = 0;

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.BINARY;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 5864918707454038001L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * element.
     *
     * @param name
     *            The name for the BSON array.
     * @param subType
     *            The sub-type of the binary data.
     * @param bytesLength
     *            The length of the binary array.
     * @return The size of the element when encoded in bytes.
     */
    private static long computeSize(final String name, final byte subType,
            final int bytesLength) {
        long result = 7; // type (1) + name null byte (1) + data length (4) +
        // subtype (1).
        result += StringEncoder.utf8Size(name);
        result += bytesLength;

        if (subType == 2) {
            // Extra length field in subtype 2.
            result += 4;
        }

        return result;
    }

    /** The sub-type of the binary data. */
    private final byte mySubType;

    /** The BSON binary value. */
    private final byte[] myValue;

    /**
     * Constructs a new {@link BinaryElement}.
     *
     * @param name
     *            The name for the BSON binary.
     * @param subType
     *            The sub-type of the binary data.
     * @param input
     *            The stream to read the data from.
     * @param length
     *            The number of bytes of data to read.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     * @throws IOException
     *             If there is an error reading from {@code input} stream.
     */
    public BinaryElement(final String name, final byte subType,
            final BsonInputStream input, final int length) throws IOException {
        this(name, subType, input, length, computeSize(name, subType, length));
    }

    /**
     * Constructs a new {@link BinaryElement}.
     *
     * @param name
     *            The name for the BSON binary.
     * @param subType
     *            The sub-type of the binary data.
     * @param input
     *            The stream to read the data from.
     * @param length
     *            The number of bytes of data to read.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link BinaryElement#BinaryElement(String, byte, BsonInputStream, int)}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     * @throws IOException
     *             If there is an error reading from {@code input} stream.
     */
    public BinaryElement(final String name, final byte subType,
            final BsonInputStream input, final int length, final long size)
            throws IOException {
        super(name, size);

        mySubType = subType;
        myValue = new byte[length];

        input.readFully(myValue);
    }

    /**
     * Constructs a new {@link BinaryElement}.
     *
     * @param name
     *            The name for the BSON binary.
     * @param subType
     *            The sub-type of the binary data.
     * @param value
     *            The BSON binary value.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public BinaryElement(final String name, final byte subType,
            final byte[] value) {
        this(name, subType, value, computeSize(name, subType,
                (value == null) ? 0 : value.length));
    }

    /**
     * Constructs a new {@link BinaryElement}.
     *
     * @param name
     *            The name for the BSON binary.
     * @param subType
     *            The sub-type of the binary data.
     * @param value
     *            The BSON binary value.
     * @param size
     *            The size of the element when encoded in bytes. If not known
     *            then use the
     *            {@link BinaryElement#BinaryElement(String, byte, byte[])}
     *            constructor instead.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public BinaryElement(final String name, final byte subType,
            final byte[] value, final long size) {
        super(name, size);

        assertNotNull(value,
                "Binary element's value cannot be null.  Add a null element instead.");

        mySubType = subType;
        myValue = value.clone();
    }

    /**
     * Constructs a new {@link BinaryElement}. Uses the
     * {@link #DEFAULT_SUB_TYPE}.
     *
     * @param name
     *            The name for the BSON binary.
     * @param value
     *            The BSON binary value.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public BinaryElement(final String name, final byte[] value) {
        this(name, DEFAULT_SUB_TYPE, value);
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitBinary} method.
     *
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitBinary(getName(), getSubType(), getValue());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the sub-types and bytes if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final BinaryElement other = (BinaryElement) otherElement;

            result = mySubType - other.mySubType;
            if (result == 0) {
                final int length = Math.min(myValue.length,
                        other.myValue.length);
                for (int i = 0; i < length; ++i) {
                    result = myValue[i] - other.myValue[i];
                    if (result != 0) {
                        return result;
                    }
                }

                result = myValue.length - other.myValue.length;
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
            final BinaryElement other = (BinaryElement) object;

            result = super.equals(object) && (mySubType == other.mySubType)
                    && Arrays.equals(myValue, other.myValue);
        }
        return result;
    }

    /**
     * Returns the byte value at the specified offset.
     *
     * @param offset
     *            The offset of the desired value.
     * @return The byte value at the offset.
     * @throws ArrayIndexOutOfBoundsException
     *             If the offset is not in the range [0, {@link #length()}).
     */
    public final byte get(final int offset) {
        return myValue[offset];
    }

    /**
     * Return the binary sub-type.
     *
     * @return The binary sub-type.
     */
    public byte getSubType() {
        return mySubType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * Returns the BSON binary value. For safety reasons this method clones the
     * internal byte array. To avoid the copying of the bytes use the
     * {@link #length()} and {@link #get(int)} methods to access each byte
     * value.
     *
     * @return The BSON binary value.
     */
    public byte[] getValue() {
        return myValue.clone();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a byte[].
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. The sub type is lost in this conversion to an {@link Object}.
     * </p>
     * <p>
     * <em>Implementation Note:</em> The return type cannot be a byte[] here as
     * {@link UuidElement} returns a {@link java.util.UUID}.
     * </p>
     */
    @Override
    public Object getValueAsObject() {
        return getValue();
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
        result = (31 * result) + mySubType;
        result = (31 * result) + Arrays.hashCode(myValue);
        return result;
    }

    /**
     * Returns the length of the contained byte array.
     *
     * @return The length of the contained byte array.
     */
    public final int length() {
        return myValue.length;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link BinaryElement}.
     * </p>
     */
    @Override
    public BinaryElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new BinaryElement(name, mySubType, myValue);
    }
}
