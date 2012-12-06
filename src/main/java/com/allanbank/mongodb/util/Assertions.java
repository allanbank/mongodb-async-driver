/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

/**
 * Assertions provides common validation methods for the driver.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Assertions {

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
     * Creates a new Assertions.
     */
    private Assertions() {
        // Static class.
    }
}
