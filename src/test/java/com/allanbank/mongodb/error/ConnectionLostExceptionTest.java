/*
 * #%L
 * ConnectionLostExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * ConnectionLostExceptionTest provides tests for the
 * {@link ConnectionLostException} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConnectionLostExceptionTest {

    /**
     * Test method for {@link ConnectionLostException#ConnectionLostException()}
     * .
     */
    @Test
    public void testConnectionLostException() {
        final ConnectionLostException exception = new ConnectionLostException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
    }

    /**
     * Test method for
     * {@link ConnectionLostException#ConnectionLostException(String)} .
     */
    @Test
    public void testConnectionLostExceptionString() {
        final ConnectionLostException exception = new ConnectionLostException(
                "foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link ConnectionLostException#ConnectionLostException(String, Throwable)}
     * .
     */
    @Test
    public void testConnectionLostExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final ConnectionLostException exception = new ConnectionLostException(
                "foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link ConnectionLostException#ConnectionLostException(Throwable)} .
     */
    @Test
    public void testConnectionLostExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final ConnectionLostException exception = new ConnectionLostException(t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

}
