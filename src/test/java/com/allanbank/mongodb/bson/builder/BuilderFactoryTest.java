/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder;

import static org.junit.Assert.*;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * BuilderFactoryTest provides tests for the {@link BuilderFactory} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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

}
