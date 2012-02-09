/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * MongoDbExceptionTest provides tests for the {@link MongoDbException} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbExceptionTest {

    /**
     * Test method for {@link MongoDbException#MongoDbException()}.
     */
    @Test
    public void testMongoDbException() {
        final MongoDbException exception = new MongoDbException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
    }

    /**
     * Test method for {@link MongoDbException#MongoDbException(String)} .
     */
    @Test
    public void testMongoDbExceptionString() {
        final MongoDbException exception = new MongoDbException("foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for
     * {@link MongoDbException#MongoDbException(String, Throwable)} .
     */
    @Test
    public void testMongoDbExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final MongoDbException exception = new MongoDbException("foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

    /**
     * Test method for {@link MongoDbException#MongoDbException(Throwable)} .
     */
    @Test
    public void testMongoDbExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final MongoDbException exception = new MongoDbException(t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
    }

}
