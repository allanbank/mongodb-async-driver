/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * JsonParseExceptionTest provides tests for the {@link JsonParseException}
 * class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
    }

    /**
     * Test method for {@link JsonParseException#JsonParseException(String)} .
     */
    @Test
    public void testJsonParseExceptionString() {
        final JsonParseException exception = new JsonParseException("foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
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
    }

}
