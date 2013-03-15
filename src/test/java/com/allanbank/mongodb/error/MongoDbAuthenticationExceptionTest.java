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
 * MongoDbAuthenticationExceptionTest provides tests for the
 * {@link MongoDbAuthenticationException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbAuthenticationExceptionTest {

    /**
     * Test method for
     * {@link MongoDbAuthenticationException#MongoDbAuthenticationException()}.
     */
    @Test
    public void testMongoDbAuthenticationException() {
        final MongoDbAuthenticationException exception = new MongoDbAuthenticationException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticationException#MongoDbAuthenticationException(String)}
     * .
     */
    @Test
    public void testMongoDbAuthenticationExceptionString() {
        final MongoDbAuthenticationException exception = new MongoDbAuthenticationException(
                "foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticationException#MongoDbAuthenticationException(String, Throwable)}
     * .
     */
    @Test
    public void testMongoDbAuthenticationExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final MongoDbAuthenticationException exception = new MongoDbAuthenticationException(
                "foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticationException#MongoDbAuthenticationException(Throwable)}
     * .
     */
    @Test
    public void testMongoDbAuthenticationExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final MongoDbAuthenticationException exception = new MongoDbAuthenticationException(
                t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

}
