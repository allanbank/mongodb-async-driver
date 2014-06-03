/*
 * #%L
 * CannotConnectExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * CannotConnectExceptionTest provides tests for the
 * {@link CannotConnectException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CannotConnectExceptionTest {

    /**
     * Test method for {@link CannotConnectException#CannotConnectException()}.
     */
    @Test
    public void testCannotConnectException() {
        final CannotConnectException exception = new CannotConnectException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
    }

    /**
     * Test method for
     * {@link CannotConnectException#CannotConnectException(String)} .
     */
    @Test
    public void testCannotConnectExceptionString() {
        final CannotConnectException exception = new CannotConnectException(
                "foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link CannotConnectException#CannotConnectException(String, Throwable)}
     * .
     */
    @Test
    public void testCannotConnectExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final CannotConnectException exception = new CannotConnectException(
                "foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link CannotConnectException#CannotConnectException(Throwable)} .
     */
    @Test
    public void testCannotConnectExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final CannotConnectException exception = new CannotConnectException(t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

}
