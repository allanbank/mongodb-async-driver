/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.Arrays;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON binary.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BinaryElement extends AbstractElement {

    /** The sub type used when no sub type is specified. */
    public static final byte DEFAULT_SUB_TYPE = 0;

    /** The BSON type for a binary. */
    public static final ElementType TYPE = ElementType.BINARY;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 5864918707454038001L;

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
     * @param value
     *            The BSON binary value.
     * @throws IllegalArgumentException
     *             If the <code>value</code> is null.
     */
    public BinaryElement(final String name, final byte subType,
            final byte[] value) {
        super(TYPE, name);
        mySubType = subType;
        if (value != null) {
            myValue = value.clone();
        }
        else {
            throw new IllegalArgumentException(
                    "Binary element value cannot be null.  Add a "
                            + "null element instead.");
        }
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
     *             If the <code>value</code> is null.
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
     * @return the subType
     */
    public byte getSubType() {
        return mySubType;
    }

    /**
     * Returns the BSON binary value.
     * 
     * @return The BSON binary value.
     */
    public byte[] getValue() {
        return myValue.clone();
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
        builder.append("\" : (0x");

        String hex = Integer.toHexString(mySubType & 0xFF);
        if (hex.length() <= 1) {
            builder.append('0');
        }
        builder.append(hex);

        builder.append(") 0x");
        for (final int element : myValue) {
            hex = Integer.toHexString(element & 0xFF);
            if (hex.length() <= 1) {
                builder.append('0');
            }
            builder.append(hex);
        }

        return builder.toString();
    }
}
