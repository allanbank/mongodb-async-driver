/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * MapReduceTest provides tests for the {@link MapReduce} command.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapReduceTest {

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduce() {
        final Document query = Find.ALL;
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = MapReduce.builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);
        builder.setFinalizeFunction("finalize");
        builder.jsMode(true);
        builder.keepTemp(true);
        builder.setLimit(10);
        builder.setQuery(query);
        builder.setScope(scope);
        builder.setSort(sort);
        builder.verbose(true);

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
    public void testMapReduceFluent() {
        final Document query = Find.ALL;
        final Document scope = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Document sort = BuilderFactory.start().addBoolean("foo", false)
                .build();

        final MapReduce.Builder builder = MapReduce.builder();
        builder.map("map");
        builder.reduce("reduce");
        builder.outputType(MapReduce.OutputType.INLINE);
        builder.finalize("finalize");
        builder.jsMode();
        builder.keepTemp();
        builder.limit(10);
        builder.query(query);
        builder.scope(scope);
        builder.sort(sort);
        builder.verbose();

        MapReduce mr = builder.build();
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

        boolean built = false;
        try {
            builder.reset().build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a MapReduce command without a map.",
                built);
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        mr = builder.build();
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

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a MapReduce command without a map.",
                built);
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceMissingReduce() {

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a MapReduce command without a reduce.",
                built);
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceOutputDb() {
        final Document query = Find.ALL;
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
        builder.outputName("coll");
        builder.outputDatabase("db");

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
        final Document query = Find.ALL;
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

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a MapReduce command without a output ono-inline.",
                built);
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceOutputDbNoCollection() {
        final Document query = Find.ALL;
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

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a MapReduce command without a output ono-inline.",
                built);
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceWithReadPreference() {

        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.outputType(MapReduce.OutputType.INLINE);
        builder.readPreference(ReadPreference.PRIMARY);

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
        assertSame(ReadPreference.PRIMARY, mr.getReadPreference());
    }

    /**
     * Test method for {@link MapReduce#MapReduce}.
     */
    @Test
    public void testMapReduceWithSort() {
        final Document query = Find.ALL;
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
        builder.sort(Sort.asc("foo"), Sort.desc("bar"));
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
