/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.connection.Message;

/**
 * MongoClientClosedExceptionTest provides tests for the
 * {@link MongoClientClosedException} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientClosedExceptionTest {

    /**
     * Test method for
     * {@link MongoClientClosedException#MongoClientClosedException()}.
     */
    @Test
    public void testMongoClientClosedException() {
        final MongoClientClosedException exception = new MongoClientClosedException();
        assertNull(exception.getCause());
        assertNull(exception.getMessage());
        assertNull(exception.getSentMessage());
    }

    /**
     * Test method for
     * {@link MongoClientClosedException#MongoClientClosedException(Message)}.
     */
    @Test
    public void testMongoClientClosedExceptionMessage() {
        final Message message = EasyMock.createMock(Message.class);

        final MongoClientClosedException exception = new MongoClientClosedException(
                message);
        assertNull(exception.getCause());
        assertEquals("MongoClient has been closed.", exception.getMessage());
        assertSame(message, exception.getSentMessage());
    }

    /**
     * Test method for
     * {@link MongoClientClosedException#MongoClientClosedException(String)} .
     */
    @Test
    public void testMongoClientClosedExceptionString() {
        final MongoClientClosedException exception = new MongoClientClosedException(
                "foo");
        assertNull(exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertNull(exception.getSentMessage());
    }

    /**
     * Test method for
     * {@link MongoClientClosedException#MongoClientClosedException(String, Throwable)}
     * .
     */
    @Test
    public void testMongoClientClosedExceptionStringThrowable() {
        final Throwable t = new Throwable();

        final MongoClientClosedException exception = new MongoClientClosedException(
                "foo", t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertNull(exception.getSentMessage());
    }

    /**
     * Test method for
     * {@link MongoClientClosedException#MongoClientClosedException(Throwable)}
     * .
     */
    @Test
    public void testMongoClientClosedExceptionThrowable() {
        final Throwable t = new Throwable("foo");

        final MongoClientClosedException exception = new MongoClientClosedException(
                t);
        assertSame(t, exception.getCause());
        assertEquals("foo", exception.getMessage());
        assertNull(exception.getSentMessage());
    }
}
