/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of the possible BSON types.
 *
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ElementType {

    /** The BSON array type. */
    ARRAY((byte) 0x04),

    /** The BSON binary type. */
    BINARY((byte) 0x05),

    /** The BSON boolean type. */
    BOOLEAN((byte) 0x08),

    /**
     * The BSON DB Pointer type.
     *
     * @deprecated See BSON specification.
     */
    @Deprecated
    DB_POINTER((byte) 0x0C),

    /** The BSON document type. */
    DOCUMENT((byte) 0x03),

    /** The BSON double type. */
    DOUBLE((byte) 0x01),

    /** The BSON 32-bit singed integer type. */
    INTEGER((byte) 0x10),

    /** The BSON JavaScript type. */
    JAVA_SCRIPT((byte) 0x0D),

    /** The BSON JavaScript w/ scope type. */
    JAVA_SCRIPT_WITH_SCOPE((byte) 0x0F),

    /** The BSON 32-bit singed integer (long) type. */
    LONG((byte) 0x12),

    /** The BSON MAX key type. */
    MAX_KEY((byte) 0x7F),

    /** The BSON MIN key type. */
    MIN_KEY((byte) 0xFF),

    /** The BSON MongoDB Timestamp type. */
    MONGO_TIMESTAMP((byte) 0x11),

    /** The BSON null type. */
    NULL((byte) 0x0A),

    /** The BSON ObjectIdElement type. */
    OBJECT_ID((byte) 0x07),

    /** The BSON regular expression type. */
    REGEX((byte) 0x0B),

    /** The BSON string type. */
    STRING((byte) 0x02),

    /** The BSON Symbol type. */
    SYMBOL((byte) 0x0E),

    /** The BSON UTC Timestamp type. */
    UTC_TIMESTAMP((byte) 0x09);

    /**
     * Provides the ordering of the types as applied by MongoDB internally. The
     * bulk of this ordering was determined from the <a href=
     * "http://docs.mongodb.org/manual/faq/developers/#what-is-the-compare-order-for-bson-types"
     * >MongoDB FAQ Entry</a> with non-listed types from the BSON Specification
     * determine experimentally.
     *
     * @see <a
     *      href="http://docs.mongodb.org/manual/faq/developers/#what-is-the-compare-order-for-bson-types">MongoDB
     *      FAQ Entry</a>
     */
    private static final Map<ElementType, Integer> ourMongoDbOrdering;

    static {
        final Map<ElementType, Integer> mongoDbOrdering = new HashMap<ElementType, Integer>(
                (int) Math.ceil(values().length / 0.75));

        int ordinal = 0;

        mongoDbOrdering.put(ElementType.MIN_KEY, Integer.valueOf(ordinal));
        ordinal += 1;

        mongoDbOrdering.put(ElementType.NULL, Integer.valueOf(ordinal));
        ordinal += 1;

        // Note - same value....
        mongoDbOrdering.put(ElementType.DOUBLE, Integer.valueOf(ordinal));
        mongoDbOrdering.put(ElementType.INTEGER, Integer.valueOf(ordinal));
        mongoDbOrdering.put(ElementType.LONG, Integer.valueOf(ordinal));
        ordinal += 1;

        // Note - same value....
        mongoDbOrdering.put(ElementType.SYMBOL, Integer.valueOf(ordinal));
        mongoDbOrdering.put(ElementType.STRING, Integer.valueOf(ordinal));
        ordinal += 1;

        mongoDbOrdering.put(ElementType.DOCUMENT, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.ARRAY, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.BINARY, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.OBJECT_ID, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.BOOLEAN, Integer.valueOf(ordinal));
        ordinal += 1;

        // Note - same value....
        mongoDbOrdering
                .put(ElementType.UTC_TIMESTAMP, Integer.valueOf(ordinal));
        mongoDbOrdering.put(ElementType.MONGO_TIMESTAMP,
                Integer.valueOf(ordinal));
        ordinal += 1;

        mongoDbOrdering.put(ElementType.REGEX, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.DB_POINTER, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.JAVA_SCRIPT, Integer.valueOf(ordinal));
        ordinal += 1;
        mongoDbOrdering.put(ElementType.JAVA_SCRIPT_WITH_SCOPE,
                Integer.valueOf(ordinal));
        ordinal += 1;

        mongoDbOrdering.put(ElementType.MAX_KEY, Integer.valueOf(ordinal));
        ordinal += 1;

        ourMongoDbOrdering = Collections.unmodifiableMap(mongoDbOrdering);
    }

    /**
     * Returns the ElementType with the provided token or <code>null</code> if
     * it is not found.
     *
     * @param token
     *            The BSON type token to find the ElementType for.
     * @return The ElementType with the provided token or <code>null</code> if
     *         it is not found.
     */
    public static ElementType valueOf(final byte token) {
        switch (token) {
        case 0x01: {
            return DOUBLE;
        }
        case 0x02: {
            return STRING;
        }
        case 0x03: {
            return DOCUMENT;
        }
        case 0x04: {
            return ARRAY;
        }
        case 0x05: {
            return BINARY;
        }
        // 0x06 not used.
        case 0x07: {
            return OBJECT_ID;
        }
        case 0x08: {
            return BOOLEAN;
        }
        case 0x09: {
            return UTC_TIMESTAMP;
        }
        case 0x0A: {
            return NULL;
        }
        case 0x0B: {
            return REGEX;
        }
        case 0x0C: {
            return DB_POINTER;
        }
        case 0x0D: {
            return JAVA_SCRIPT;
        }
        case 0x0E: {
            return SYMBOL;
        }
        case 0x0F: {
            return JAVA_SCRIPT_WITH_SCOPE;
        }
        case 0x10: {
            return INTEGER;
        }
        case 0x11: {
            return MONGO_TIMESTAMP;
        }
        case 0x12: {
            return LONG;
        }
        case 0x7F: {
            return MAX_KEY;
        }
        case (byte) 0xFF: {
            return MIN_KEY;
        }
        default: {
            for (final ElementType type : values()) {
                if (token == type.getToken()) {
                    return type;
                }
            }

            return null;
        }
        }
    }

    /** The token for the BSON type. */
    private final byte myToken;

    /**
     * Create a new {@link ElementType}.
     *
     * @param token
     *            The token for the {@link ElementType}.
     */
    private ElementType(final byte token) {
        myToken = token;
    }

    /**
     * Similar to {@link #compareTo} but instead of comparing on the ordinal
     * value compares the values based on the MongoDB sort order.
     *
     * @param rhs
     *            The right-hand-side of the ordering.
     * @return A negative value if this {@link ElementType} is less than the
     *         {@code rhs}, zero if they are equal, and a positive value if it
     *         is greater than the {@code rhs}.
     */
    public int compare(final ElementType rhs) {

        final int lhsValue = ourMongoDbOrdering.get(this).intValue();
        final int rhsValue = ourMongoDbOrdering.get(rhs).intValue();

        return lhsValue - rhsValue;
    }

    /**
     * Returns the token for the BSON type.
     *
     * @return The token for the BSON type.
     */
    public byte getToken() {
        return myToken;
    }

}
