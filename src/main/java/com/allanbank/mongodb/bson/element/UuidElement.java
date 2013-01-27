/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.util.UUID;

import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.util.IOUtils;

/**
 * UuidElement provides a helper element for handling UUID {@link BinaryElement}
 * sub-types.
 * <p>
 * If no sub-type is provided this class defaults to the standardized sub-type 4
 * binary element which encodes the UUID from most significant byte to least
 * significant byte. If the deprecated sub-type 3 is specified this class
 * assumes the legacy Java encoding of the UUID which encodes the most
 * significant long in least-significant-byte order and then the least
 * significant long in least-significant-byte order.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UuidElement extends BinaryElement {

    /**
     * The legacy (reverse byte order for high and low long values) subtype for
     * the UUID.
     */
    public static final byte LEGACY_UUID_SUBTTYPE = 3;

    /** The length for the UUID binary value. */
    public static final int UUID_BINARY_LENGTH = 16;

    /** The default subtype for the UUID. */
    public static final byte UUID_SUBTTYPE = 4;

    /** The serialization version for the class. */
    private static final long serialVersionUID = 6461067538910973839L;

    /**
     * Converts the UUID value to a byte array based on the subtype.
     * 
     * @param uuidSubttype
     *            The subtype for the UUID encoding.
     * @param value
     *            The UUID value to convert.
     * @return The byte encoding.
     */
    private static byte[] toBytes(final byte uuidSubttype, final UUID value) {
        assertNotNull(value,
                "The UUID value for a UuidElement must not be null.");

        long high = value.getMostSignificantBits();
        long low = value.getLeastSignificantBits();

        if (uuidSubttype == LEGACY_UUID_SUBTTYPE) {
            high = EndianUtils.swap(high);
            low = EndianUtils.swap(low);
        }

        final byte[] result = new byte[16];

        result[0] = (byte) ((high >> 56) & 0xFF);
        result[1] = (byte) ((high >> 48) & 0xFF);
        result[2] = (byte) ((high >> 40) & 0xFF);
        result[3] = (byte) ((high >> 32) & 0xFF);
        result[4] = (byte) ((high >> 24) & 0xFF);
        result[5] = (byte) ((high >> 16) & 0xFF);
        result[6] = (byte) ((high >> 8) & 0xFF);
        result[7] = (byte) (high & 0xFF);
        result[8] = (byte) ((low >> 56) & 0xFF);
        result[9] = (byte) ((low >> 48) & 0xFF);
        result[10] = (byte) ((low >> 40) & 0xFF);
        result[11] = (byte) ((low >> 32) & 0xFF);
        result[12] = (byte) ((low >> 24) & 0xFF);
        result[13] = (byte) ((low >> 16) & 0xFF);
        result[14] = (byte) ((low >> 8) & 0xFF);
        result[15] = (byte) (low & 0xFF);

        return result;
    }

    /** The UUID value created. */
    private final UUID myUuid;

    /**
     * Creates a new UuidElement.
     * 
     * @param name
     *            The name for the element.
     * @param subType
     *            The subtype for the UUID element.
     * @param value
     *            The UUID bytes for the element.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>. If
     *             the subType is not {@link #UUID_SUBTTYPE} or
     *             {@link #LEGACY_UUID_SUBTTYPE}. If the value is not a 16 bytes
     *             long.
     */
    public UuidElement(final String name, final byte subType, final byte[] value) {
        super(name, subType, value);

        myUuid = toUuid(subType, value);
    }

    /**
     * Creates a new UuidElement.
     * 
     * @param name
     *            The name for the element.
     * @param subType
     *            The subtype for the UUID element.
     * @param value
     *            The UUID value for the element.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public UuidElement(final String name, final byte subType, final UUID value) {
        super(name, subType, toBytes(subType, value));

        myUuid = value;
    }

    /**
     * Creates a new UuidElement.
     * 
     * @param name
     *            The name for the element.
     * @param value
     *            The UUID value for the element.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public UuidElement(final String name, final UUID value) {
        super(name, UUID_SUBTTYPE, toBytes(UUID_SUBTTYPE, value));

        myUuid = value;
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
            final UuidElement other = (UuidElement) object;

            result = super.equals(object) && myUuid.equals(other.myUuid);
        }
        return result;
    }

    /**
     * Returns the {@link UUID} value.
     * 
     * @return The {@link UUID} value.
     */
    public UUID getUuid() {
        return myUuid;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the UUID value.
     * </p>
     * <p>
     * <b>Note:</b> This value will not be recreated is a Object-->Element
     * conversion. The sub type is lost in this conversion to an {@link Object}.
     * </p>
     */
    @Override
    public UUID getValueAsObject() {
        return myUuid;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the result of the {@link UUID#toString()}.
     * </p>
     */
    @Override
    public String getValueAsString() {
        return myUuid.toString();
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
        result = (31 * result) + myUuid.hashCode();
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link BinaryElement}.
     * </p>
     */
    @Override
    public UuidElement withName(final String name) {
        return new UuidElement(name, getSubType(), myUuid);
    }

    /**
     * Converts the UUID binary form into a UUID object.
     * 
     * @param subType
     *            The sub-type for the UUID encoding.
     * @param value
     *            The encoded UUID.
     * @return The UUID encoded in the {@code value}.
     * @throws IllegalArgumentException
     *             If the length of the {@code value} array is not
     *             {@value #UUID_BINARY_LENGTH}.
     */
    private UUID toUuid(final byte subType, final byte[] value)
            throws IllegalArgumentException {

        if (value.length == UUID_BINARY_LENGTH) {
            long high = 0;
            long low = 0;

            for (int i = 0; i < 8; ++i) {
                high <<= Byte.SIZE;
                high += (value[i] & 0xFF);
            }
            for (int i = 8; i < 16; ++i) {
                low <<= Byte.SIZE;
                low += (value[i] & 0xFF);
            }

            if (subType == LEGACY_UUID_SUBTTYPE) {
                high = EndianUtils.swap(high);
                low = EndianUtils.swap(low);
            }
            return new UUID(high, low);
        }

        throw new IllegalArgumentException(
                "The value for a UUID must be 16 bytes long: "
                        + IOUtils.toHex(value));
    }
}
