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
