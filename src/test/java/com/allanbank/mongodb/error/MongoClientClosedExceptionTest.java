/*
 * #%L
 * MongoClientClosedExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.client.Message;

/**
 * MongoClientClosedExceptionTest provides tests for the
 * {@link MongoClientClosedException} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
