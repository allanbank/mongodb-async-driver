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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * FindTest provides tests for the {@link Find} command.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FindTest {

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFind() {
        final Document query = BuilderFactory.start().build();
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();
        final Document sort = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.setSort(sort);

        Find request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertSame(sort, request.getSort());

        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindMinimal() {
        final Document query = BuilderFactory.start().build();

        final Find.Builder builder = new Find.Builder(query);

        final Find request = builder.build();
        assertSame(query, request.getQuery());
        assertNull(request.getReturnFields());
        assertEquals(0, request.getBatchSize());
        assertEquals(0, request.getLimit());
        assertEquals(0, request.getNumberToSkip());
        assertFalse(request.isPartialOk());
        assertNull(request.getReadPreference());
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindNoQuery() {
        final Find.Builder builder = new Find.Builder();
        try {
            builder.build();
            fail("Should have failed to create a Find command without a query.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link Find#Find}.
     */
    @Test
    public void testFindWithSort() {
        final Document query = BuilderFactory.start().build();
        final Document fields = BuilderFactory.start().addInteger("foo", 3)
                .build();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(query);
        builder.setReturnFields(fields);
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.setSort(Sort.desc("f"));

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

        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(fields, request.getReturnFields());
        assertEquals(101010, request.getBatchSize());
        assertEquals(202020, request.getLimit());
        assertEquals(123456, request.getNumberToSkip());
        assertTrue(request.isPartialOk());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());
    }
}
