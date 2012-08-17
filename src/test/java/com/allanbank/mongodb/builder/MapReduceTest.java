/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * MapReduceTest provides tests for the {@link MapReduce} command.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapReduceTest {

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduce() {
        final Document query = BuilderFactory.start().build();
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(sort);
        builder.setVerbose(true);

        final MapReduce mr = builder.build();
        assertEquals("map", mr.getMapFunction());
        assertEquals("reduce", mr.getReduceFunction());
        assertSame(MapReduce.OutputType.INLINE, mr.getOutputType());
        assertEquals("finalize", mr.getFinalizeFunction());
        assertTrue(mr.isJsMode());
        assertTrue(mr.isKeepTemp());
        assertEquals(10, mr.getLimit());
        assertSame(query, mr.getQuery());
        assertSame(scope, mr.getScope());
        assertSame(sort, mr.getSort());
        assertTrue(mr.isVerbose());
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceMinimal() {

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final MapReduce mr = builder.build();
        assertEquals("map", mr.getMapFunction());
        assertEquals("reduce", mr.getReduceFunction());
        assertSame(MapReduce.OutputType.INLINE, mr.getOutputType());
        assertNull(mr.getFinalizeFunction());
        assertFalse(mr.isJsMode());
        assertFalse(mr.isKeepTemp());
        assertEquals(0, mr.getLimit());
        assertNull(mr.getQuery());
        assertNull(mr.getScope());
        assertNull(mr.getSort());
        assertFalse(mr.isVerbose());
        assertNull(mr.getOutputName());
        assertNull(mr.getOutputDatabase());
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceMissingMap() {

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        try {
            builder.build();
            fail("Should have failed to create a MapReduce command without a"
                    + " map function.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceMissingRap() {

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        try {
            builder.build();
            fail("Should have failed to create a MapReduce command without a"
                    + " reduce function.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceOutputDb() {
        final Document query = BuilderFactory.start().build();
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.MERGE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(sort);
        builder.setVerbose(true);
        builder.setOutputName("coll");
        builder.setOutputDatabase("db");

        final MapReduce mr = builder.build();
        assertEquals("map", mr.getMapFunction());
        assertEquals("reduce", mr.getReduceFunction());
        assertSame(MapReduce.OutputType.MERGE, mr.getOutputType());
        assertEquals("finalize", mr.getFinalizeFunction());
        assertTrue(mr.isJsMode());
        assertTrue(mr.isKeepTemp());
        assertEquals(10, mr.getLimit());
        assertSame(query, mr.getQuery());
        assertSame(scope, mr.getScope());
        assertSame(sort, mr.getSort());
        assertTrue(mr.isVerbose());
        assertEquals("coll", mr.getOutputName());
        assertEquals("db", mr.getOutputDatabase());
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceOutputDbEmptyCollection() {
        final Document query = BuilderFactory.start().build();
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.MERGE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(sort);
        builder.setVerbose(true);
        builder.setOutputName("");
        builder.setOutputDatabase("db");

        try {
            builder.build();
            fail("Should have failed to create a MapReduce command without an"
                    + " output collection on non-inline.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceOutputDbNoCollection() {
        final Document query = BuilderFactory.start().build();
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.MERGE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(sort);
        builder.setVerbose(true);
        builder.setOutputDatabase("db");

        try {
            builder.build();
            fail("Should have failed to create a MapReduce command without an"
                    + " output collection on non-inline.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceWithSort() {
        final Document query = BuilderFactory.start().build();
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addInteger("foo", 1)
                .addInteger("bar", -1).build();

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(Sort.asc("foo"), Sort.desc("bar"));
        builder.setVerbose(true);

        final MapReduce mr = builder.build();
        assertEquals("map", mr.getMapFunction());
        assertEquals("reduce", mr.getReduceFunction());
        assertSame(MapReduce.OutputType.INLINE, mr.getOutputType());
        assertEquals("finalize", mr.getFinalizeFunction());
        assertTrue(mr.isJsMode());
        assertTrue(mr.isKeepTemp());
        assertEquals(10, mr.getLimit());
        assertSame(query, mr.getQuery());
        assertSame(scope, mr.getScope());
        assertEquals(sort, mr.getSort());
        assertTrue(mr.isVerbose());
    }
}
