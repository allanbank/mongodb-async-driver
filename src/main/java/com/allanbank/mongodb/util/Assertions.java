/*
 * #%L
 * Assertions.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
