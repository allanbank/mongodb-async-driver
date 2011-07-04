/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

/**
 * Utilities to deal with integer endian differences.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class EndianUtils {
	/**
	 * Performs a byte-order swap for a 32-bit signed integer.
	 * 
	 * @param value
	 *            The value to swap.
	 * @return The swapped value.
	 */
	public static int swap(int value) {
		return (((value << 24) & 0xFF000000) | ((value << 8) & 0x00FF0000)
				| ((value >> 8) & 0x0000FF00) | ((value >> 24) & 0x000000FF));
	}

	/**
	 * Performs a byte-order swap for a 64-bit signed integer.
	 * 
	 * @param value
	 *            The value to swap.
	 * @return The swapped value.
	 */
	public static long swap(long value) {
		return (((value << 56) & 0xFF00000000000000L)
				| ((value << 40) & 0x00FF000000000000L)
				| ((value << 24) & 0x0000FF0000000000L)
				| ((value << 8) & 0x000000FF00000000L)
				| ((value >> 8) & 0x00000000FF000000L)
				| ((value >> 24) & 0x0000000000FF0000L)
				| ((value >> 40) & 0x000000000000FF00L) | ((value >> 56) & 0x00000000000000FFL));
	}

}
