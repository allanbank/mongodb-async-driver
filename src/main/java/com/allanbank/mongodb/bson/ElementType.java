/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

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
     * Returns the token for the BSON type.
     * 
     * @return The token for the BSON type.
     */
    public byte getToken() {
        return myToken;
    }

}
