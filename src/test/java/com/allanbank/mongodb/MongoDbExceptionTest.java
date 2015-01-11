/*
 * #%L
 * MongoDbExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * MongoDbExceptionTest provides tests for the {@link MongoDbException} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
