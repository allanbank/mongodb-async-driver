/*
 * #%L
 * JsonExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * JsonExceptionTest provides tests for the {@link JsonException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonExceptionTest {

    /**
     * Test method for {@link JsonException#JsonException()}.
     */
    @Test
    public void testJsonException() {
        final JsonException exception = new JsonException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
    }

    /**
     * Test method for {@link JsonException#JsonException(String)} .
     */
    @Test
    public void testJsonExceptionString() {
        final JsonException exception = new JsonException("foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for {@link JsonException#JsonException(String, Throwable)} .
     */
    @Test
    public void testJsonExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final JsonException exception = new JsonException("foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for {@link JsonException#JsonException(Throwable)} .
     */
    @Test
    public void testJsonExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final JsonException exception = new JsonException(t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

}
