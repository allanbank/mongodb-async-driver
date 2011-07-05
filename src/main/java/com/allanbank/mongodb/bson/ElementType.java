/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

/**
 * Enumeration of the possible BSON types.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ElementType {

	/** The BSON array type. */
	ARRAY((byte) 0x04),

	/** The BSON binary type. */
	BINARY((byte) 0x05),

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

	/** The BSON false type. */
	FALSE((byte) 0x08),

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
	OBJECT_ID((byte) 0x06),

	/** The BSON regular expression type. */
	REGEX((byte) 0x0B),

	/** The BSON string type. */
	STRING((byte) 0x02),

	/** The BSON Symbol type. */
	SYMBOL((byte) 0x0E),

	/** The BSON true type. */
	TRUE((byte) 0x07),

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
		for (final ElementType type : values()) {
			if (token == type.getToken()) {
				return type;
			}
		}

		return null;
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
