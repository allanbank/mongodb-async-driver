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
 * FindTest provides tests for the {@link Find} command.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FindTest {

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFind() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();
        final Document sort = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.returnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.sort(sort);
        builder.tailable();

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertSame(sort, request.getSort());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());
        assertTrue(request.isTailable());
        assertTrue(request.isAwaitData());
        assertFalse(request.isImmortalCursor());

        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setTailable(false);
        builder.setImmortalCursor(true);
        builder.snapshot(true);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertTrue(request.isSnapshot());
        assertFalse(request.isTailable());
        assertTrue(request.isAwaitData());
        assertTrue(request.isImmortalCursor());
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithReturnKeys() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().add("foo", 1)
                .add("bar", 1).build();
        final Document sort = BuilderFactory.start().add("foo", 1).build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.returnFields("foo", "bar");
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.sort(sort);
        builder.tailable();

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertEquals(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertSame(sort, request.getSort());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());
        assertTrue(request.isTailable());
        assertTrue(request.isAwaitData());
        assertFalse(request.isImmortalCursor());
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindMinimal() {
        final Find.Builder builder = new Find.Builder();

        final Find request = builder.build();

        assertSame(Find.ALL, request.getQuery());
        assertNull(request.getReturnFields());
        assertEquals(0, request.getBatchSize());
        assertEquals(0, request.getLimit());
        assertEquals(0, request.getNumberToSkip());
        assertFalse(request.isPartialOk());
        assertNull(request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());
        assertFalse(request.isTailable());
        assertFalse(request.isAwaitData());
        assertFalse(request.isImmortalCursor());
        assertFalse(request.isShowDiskLocation());
        assertFalse(request.isReturnIndexKeysOnly());
        assertEquals(-1L, request.getMaximumDocumentsToScan());
        assertNull(request.getMaximumRange());
        assertNull(request.getMinimumRange());

        assertEquals(request.getQuery(), request.toQueryRequest(false));
        assertEquals(
                BuilderFactory
                        .start()
                        .add("$query", request.getQuery())
                        .add(ReadPreference.FIELD_NAME,
                                ReadPreference.CLOSEST.asDocument()).build(),
                request.toQueryRequest(false, ReadPreference.CLOSEST));

    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithExplain() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.query(query);
        builder.setReturnFields(fields);
        builder.batchSize(101010);
        builder.limit(202020);
        builder.skip(123456);
        builder.partialOk(true);
        builder.readPreference(ReadPreference.CLOSEST);
        builder.setAwaitData(true);

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());
        assertFalse(request.isTailable());
        assertTrue(request.isAwaitData());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$explain", true).build(), request.toQueryRequest(true));

        builder.reset();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(request.getQuery(), request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithHint() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.partialOk();
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.hint("_id_1");
        builder.hint(Sort.asc("f"));

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertEquals(BuilderFactory.start().addInteger("f", 1).build(),
                request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(
                BuilderFactory
                        .start()
                        .add("$query", request.getQuery())
                        .add("$hint",
                                BuilderFactory.start().addInteger("f", 1)
                                        .build()).build(),
                request.toQueryRequest(false));

        builder.reset();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.hint(BuilderFactory.start().addInteger("f", 1));

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertEquals(BuilderFactory.start().addInteger("f", 1).build(),
                request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(
                BuilderFactory
                        .start()
                        .add("$query", request.getQuery())
                        .add("$hint",
                                BuilderFactory.start().addInteger("f", 1)
                                        .build()).build(),
                request.toQueryRequest(false));

        builder.reset();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(request.getQuery(), request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithHintName() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.partialOk();
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.setHint(Sort.asc("f"));
        builder.setHint("_id_1");

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertEquals("_id_1", request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$hint", "_id_1").build(), request.toQueryRequest(false));

        builder.reset();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(request.getQuery(), request.toQueryRequest(false));

    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithImmortalCursor() {
        final Find.Builder builder = Find.builder();
        builder.immortalCursor();

        Find request = builder.build();
        assertTrue(request.isImmortalCursor());

        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertFalse(request.isImmortalCursor());
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.immortalCursor(true);
        request = builder.build();
        assertTrue(request.isImmortalCursor());
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.immortalCursor(false);
        request = builder.build();
        assertFalse(request.isImmortalCursor());
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithMaximumDocumentsToScan() {
        final Find.Builder builder = Find.builder();
        builder.maxScan(10);

        Find request = builder.build();
        assertSame(Find.ALL, request.getQuery());
        assertEquals(10L, request.getMaximumDocumentsToScan());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$maxScan", 10L).build(), request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.setMaximumDocumentsToScan(20L);
        request = builder.build();
        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$maxScan", 20L).build(), request.toQueryRequest(false));

        builder.setMaximumDocumentsToScan(0);
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithMaximumRange() {
        final Document range = BuilderFactory.start().add("a", 1000)
                .add("b", "tag").build();

        final Find.Builder builder = Find.builder();
        builder.max(range);

        Find request = builder.build();
        assertSame(Find.ALL, request.getQuery());
        assertEquals(range, request.getMaximumRange());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$max", range).build(), request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.setMaximumRange(range);
        request = builder.build();
        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$max", range).build(), request.toQueryRequest(false));

        builder.setMaximumRange(null);
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithMinimumRange() {
        final Document range = BuilderFactory.start().add("a", 1000)
                .add("b", "tag").build();

        final Find.Builder builder = Find.builder();
        builder.min(range);

        Find request = builder.build();
        assertSame(Find.ALL, request.getQuery());
        assertEquals(range, request.getMinimumRange());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$min", range).build(), request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.setMinimumRange(range);
        request = builder.build();
        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$min", range).build(), request.toQueryRequest(false));

        builder.setMinimumRange(null);
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithReturnIndexKeysOnly() {
        final Find.Builder builder = Find.builder();
        builder.returnKey();

        Find request = builder.build();
        assertSame(Find.ALL, request.getQuery());
        assertTrue(request.isReturnIndexKeysOnly());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$returnKey", true).build(), request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.returnKey(true);
        request = builder.build();
        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$returnKey", true).build(), request.toQueryRequest(false));

        builder.returnKey(false);
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithShowDiskLocations() {

        final Find.Builder builder = Find.builder();
        builder.showDiskLoc();

        Find request = builder.build();
        assertSame(Find.ALL, request.getQuery());
        assertTrue(request.isShowDiskLocation());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$showDiskLoc", true).build(),
                request.toQueryRequest(false));

        builder.reset();
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));

        builder.showDiskLoc(true);
        request = builder.build();
        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$showDiskLoc", true).build(),
                request.toQueryRequest(false));

        builder.showDiskLoc(false);
        request = builder.build();
        assertEquals(Find.ALL, request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithSnapshot() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.partialOk();
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.snapshot();

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertTrue(request.isSnapshot());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$snapshot", true).build(), request.toQueryRequest(false));

        builder.reset();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertNull(request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertNull(request.getHint());
        assertNull(request.getHintName());
        assertFalse(request.isSnapshot());

        assertEquals(request.getQuery(), request.toQueryRequest(false));
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithSort() {
        final Document query = Find.ALL;
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.partialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.sort(Sort.desc("f"));

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertEquals(BuilderFactory.start().addInteger("f", -1).build(),
                request.getSort());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$orderby", request.getSort()).build(),
                request.toQueryRequest(false));

        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());

        assertEquals(BuilderFactory.start().add("$query", request.getQuery())
                .add("$orderby", request.getSort()).build(),
                request.toQueryRequest(false));
    }
}
