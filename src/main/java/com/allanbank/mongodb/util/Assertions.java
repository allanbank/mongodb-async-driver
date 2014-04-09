/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

/**
 * Assertions provides common validation methods for the driver.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Assertions {

    /**
     * Throws an {@link IllegalArgumentException} if the {@code value} is
     * <code>null</code> or an empty string.
     * 
     * @param value
     *            The value to test.
     * @param message
     *            The message for the exception to throw.
     * @throws IllegalArgumentException
     *             In the case that the {@code value} is <code>null</code>.
     */
    public static void assertNotEmpty(final String value, final String message)
            throws IllegalArgumentException {
        if ((value == null) || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Throws an {@link IllegalArgumentException} if the {@code value} is
     * <code>null</code>.
     * 
     * @param value
     *            The value to test.
     * @param message
     *            The message for the exception to throw.
     * @throws IllegalArgumentException
     *             In the case that the {@code value} is <code>null</code>.
     */
    public static void assertNotNull(final Object value, final String message)
            throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Throws an {@link IllegalArgumentException} if the {@code value} is
     * <code>null</code>.
     * 
     * @param mustBeTrue
     *            The value to test.
     * @param message
     *            The message for the exception to throw.
     * @throws IllegalArgumentException
     *             In the case that the {@code value} is <code>null</code>.
     */
    public static void assertThat(final boolean mustBeTrue, final String message)
            throws IllegalArgumentException {
        if (!mustBeTrue) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Creates a new Assertions.
     */
    private Assertions() {
        // Static class.
    }
}
