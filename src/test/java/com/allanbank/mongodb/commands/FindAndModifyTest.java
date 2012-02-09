/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.commands;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * FindAndModifyTest provides tests for the {@link FindAndModify} command.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FindAndModifyTest {

    /**
     * Test method for {@link FindAndModify#FindAndModify}.
     */
    @Test
    public void testFindAndModify() {
        final Document query = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 3)
                .get();
        final Document sort = BuilderFactory.start().addInteger("foo", 3).get();

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query);
        builder.setUpdate(update);
        builder.setRemove(true);
        builder.setReturnNew(true);
        builder.setSort(sort);
        builder.setUpsert(true);

        final FindAndModify request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(update, request.getUpdate());
        assertSame(sort, request.getSort());
        assertTrue(request.isRemove());
        assertTrue(request.isReturnNew());
        assertTrue(request.isUpsert());
    }

    /**
     * Test method for {@link FindAndModify#FindAndModify}.
     */
    @Test
    public void testFindAndModifyMinimal() {
        final Document query = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 3)
                .get();

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query);
        builder.setUpdate(update);

        final FindAndModify request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(update, request.getUpdate());
        assertNull(request.getSort());
        assertFalse(request.isRemove());
        assertFalse(request.isReturnNew());
        assertFalse(request.isUpsert());
    }

    /**
     * Test method for {@link FindAndModify#FindAndModify}.
     */
    @Test
    public void testFindAndModifyNoQuery() {
        final Document update = BuilderFactory.start().addInteger("foo", 3)
                .get();

        final FindAndModify.Builder builder = new FindAndModify.Builder();

        builder.setUpdate(update);

        try {
            builder.build();
            fail("Should have failed to create a FindAndModify command without a query.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

    /**
     * Test method for {@link FindAndModify#FindAndModify}.
     */
    @Test
    public void testFindAndModifyNoUpdate() {
        final Document query = BuilderFactory.start().get();

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query);

        try {
            builder.build();
            fail("Should have failed to create a FindAndModify command without an update.");
        }
        catch (final AssertionError expected) {
            // Good.
        }
    }

}
