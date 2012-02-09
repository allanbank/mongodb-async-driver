/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * DistinctTest provides tests for the {@link Distinct} command.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DistinctTest {

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinct() {
        final Document doc = BuilderFactory.start().get();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(doc);

        final Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertSame(doc, d.getQuery());
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test()
    public void testDistinctEmptyKey() {
        final Document doc = BuilderFactory.start().get();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("");
        builder.setQuery(doc);

        try {
            builder.build();
            fail("Should have failed to create a distinct command without a key.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test()
    public void testDistinctNoKey() {
        final Document doc = BuilderFactory.start().get();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setQuery(doc);

        try {
            builder.build();
            fail("Should have failed to create a distinct command without a key.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinctNoQuery() {

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");

        final Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertNull(d.getQuery());
    }

}
