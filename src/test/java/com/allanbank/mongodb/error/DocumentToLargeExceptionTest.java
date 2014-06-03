/*
 * #%L
 * DocumentToLargeExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.junit.Assert.assertSame;

import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * DocumentToLargeExceptionTest provides tests for the
 * {@link DocumentToLargeException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentToLargeExceptionTest {

    /**
     * Test method for
     * {@link DocumentToLargeException#DocumentToLargeException(int, int, Document)}
     * .
     */
    @Test
    public void testDocumentToLargeException() {
        final Random rand = new Random(System.currentTimeMillis());

        final int size = rand.nextInt();
        final int max = rand.nextInt();
        final Document doc = BuilderFactory.start().build();

        final DocumentToLargeException ex = new DocumentToLargeException(size,
                max, doc);

        assertEquals("Attempted to serialize a document of size " + size
                + " when current maximum is " + max + ".", ex.getMessage());
        assertEquals(size, ex.getSize());
        assertEquals(max, ex.getMaximumSize());
        assertSame(doc, ex.getDocument());
    }
}
