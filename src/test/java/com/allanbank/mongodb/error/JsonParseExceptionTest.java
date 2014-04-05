/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * JsonParseExceptionTest provides tests for the {@link JsonParseException}
 * class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonParseExceptionTest {

    /**
     * Test method for {@link JsonParseException#JsonParseException()}.
     */
    @Test
    public void testJsonParseException() {
        final JsonParseException exception = new JsonParseException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
        assertEquals(-1, exception.getColumn());
        assertEquals(-1, exception.getLine());
    }

    /**
     * Test method for {@link JsonParseException#JsonParseException(String)} .
     */
    @Test
    public void testJsonParseExceptionString() {
        final JsonParseException exception = new JsonParseException("foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertEquals(-1, exception.getColumn());
        assertEquals(-1, exception.getLine());
    }

    /**
     * Test method for
     * {@link JsonParseException#JsonParseException(String, int, int)} .
     */
    @Test
    public void testJsonParseExceptionStringIntInt() {
        final JsonParseException exception = new JsonParseException("foo", 41,
                51);
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertEquals(51, exception.getColumn());
        assertEquals(41, exception.getLine());
    }

    /**
     * Test method for
     * {@link JsonParseException#JsonParseException(String, Throwable)} .
     */
    @Test
    public void testJsonParseExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final JsonParseException exception = new JsonParseException("foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertEquals(-1, exception.getColumn());
        assertEquals(-1, exception.getLine());
    }

    /**
     * Test method for
     * {@link JsonParseException#JsonParseException(String, Throwable, int, int)}
     * .
     */
    @Test
    public void testJsonParseExceptionStringThrowableIntInt() {
        final Throwable t = new Throwable();

        final JsonParseException exception = new JsonParseException("foo", t,
                123, 456);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertEquals(456, exception.getColumn());
        assertEquals(123, exception.getLine());
    }

    /**
     * Test method for {@link JsonParseException#JsonParseException(Throwable)}
     * .
     */
    @Test
    public void testJsonParseExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final JsonParseException exception = new JsonParseException(t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertEquals(-1, exception.getColumn());
        assertEquals(-1, exception.getLine());
    }
}
