/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * JsonExceptionTest provides tests for the {@link JsonException} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
