/*
 * #%L
 * AssertionsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * AssertionsTest provides test cases for the {@link Assertions} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AssertionsTest {

    /**
     * Test method for {@link Assertions#assertNotEmpty(String, String)}.
     */
    @Test
    public void testAssertNotEmptyNotThrowsOnNotNull() {
        try {
            Assertions.assertNotEmpty("f", "Not Expected!");
        }
        catch (final IllegalArgumentException e) {
            fail("Should not have thrown an IllegalArguementException");
        }
    }

    /**
     * Test method for {@link Assertions#assertNotEmpty(String, String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAssertNotEmptyThrowsOnBlank() {
        Assertions.assertNotEmpty(" ", "Expected!");
    }

    /**
     * Test method for {@link Assertions#assertNotEmpty(String, String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAssertNotEmptyThrowsOnNull() {
        Assertions.assertNotEmpty(null, "Expected!");
    }

    /**
     * Test method for {@link Assertions#assertNotNull(Object, String)}.
     */
    @Test
    public void testAssertNotNullNotThrowsOnNotNull() {
        try {
            Assertions.assertNotNull("f", "Not Expected!");
        }
        catch (final IllegalArgumentException e) {
            fail("Should not have thrown an IllegalArguementException");
        }
    }

    /**
     * Test method for {@link Assertions#assertNotNull(Object, String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAssertNotNullThrowsOnNull() {
        Assertions.assertNotNull(null, "Expected!");
    }

}
