/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * DistinctTest provides tests for the {@link Distinct} command.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DistinctTest {

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinct() {
        final Document doc = BuilderFactory.start().build();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.key("foo").query(doc);

        final Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertSame(doc, d.getQuery());
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test()
    public void testDistinctEmptyKey() {
        final Document doc = BuilderFactory.start().build();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("");
        builder.setQuery(doc);

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a distinct command without a key.",
                built);
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test()
    public void testDistinctNoKey() {
        final Document doc = BuilderFactory.start().build();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setQuery(doc);

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a distinct command without a key.",
                built);
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinctNoQuery() {

        final Distinct.Builder builder = Distinct.builder();
        builder.setKey("foo");

        final Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertNull(d.getQuery());
    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinctReset() {
        final Document doc = BuilderFactory.start().build();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo").query(doc).readPreference(ReadPreference.CLOSEST);

        Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertSame(doc, d.getQuery());
        assertSame(ReadPreference.CLOSEST, d.getReadPreference());

        builder.reset();

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a distinct command without a key.",
                built);

        builder.setKey("foo");

        d = builder.build();
        assertEquals("foo", d.getKey());
        assertNull(d.getQuery());
        assertNull(d.getReadPreference());

    }

    /**
     * Test method for {@link Distinct#Distinct} .
     */
    @Test
    public void testDistinctWithReadPreference() {
        final Document doc = BuilderFactory.start().build();

        final Distinct.Builder builder = new Distinct.Builder();
        builder.key("foo").query(doc).readPreference(ReadPreference.CLOSEST);

        final Distinct d = builder.build();
        assertEquals("foo", d.getKey());
        assertSame(doc, d.getQuery());
        assertSame(ReadPreference.CLOSEST, d.getReadPreference());
    }

}
