/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * DocumentToLargeExceptionTest provides TODO - Finish
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentToLargeExceptionTest {

    /**
     * Test method for
     * {@link DocumentToLargeException#DocumentToLargeException(int, int, Document)}
     * .
     */
    @Test
    public void testDocumentToLargeException() {
        Random rand = new Random(System.currentTimeMillis());

        int size = rand.nextInt();
        int max = rand.nextInt();
        Document doc = BuilderFactory.start().build();

        DocumentToLargeException ex = new DocumentToLargeException(size, max,
                doc);

        assertEquals("Attemted to serialize a document of size " + size
                + " when current maximum is " + max + ".", ex.getMessage());
        assertEquals(size, ex.getSize());
        assertEquals(max, ex.getMaximumSize());
        assertSame(doc, ex.getDocument());
    }
}
