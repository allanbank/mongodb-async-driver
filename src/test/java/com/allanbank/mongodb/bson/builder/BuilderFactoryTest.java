/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * BuilderFactoryTest provides tests for the {@link BuilderFactory} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuilderFactoryTest {

    /**
     * Test method for {@link BuilderFactory#start()}.
     */
    @Test
    public void testStart() {
        assertNotNull(BuilderFactory.start());
        assertTrue(BuilderFactory.start() instanceof DocumentBuilderImpl);
    }

    /**
     * Test method for {@link BuilderFactory#startArray()}.
     */
    @Test
    public void testStartArray() {
        assertNotNull(BuilderFactory.startArray());
        assertTrue(BuilderFactory.startArray() instanceof ArrayBuilderImpl);
    }

    /**
     * Test method for {@link BuilderFactory#start(DocumentAssignable)}.
     */
    @Test
    public void testStartDocumentAssignable() {
        final DocumentBuilder seedBuilder = BuilderFactory.start().add("a", 1);
        final Document seed = seedBuilder.build();

        assertNotNull(BuilderFactory.start(seed));
        assertTrue(BuilderFactory.start(seed) instanceof DocumentBuilderImpl);
        assertEquals(seed.asDocument(), BuilderFactory.start(seed).build());
        assertEquals(seedBuilder.add("b", 2).asDocument(), BuilderFactory
                .start(seed).add("b", 2).build());
    }

}
