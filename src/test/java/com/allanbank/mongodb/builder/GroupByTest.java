/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * GroupByTest provides tests for the {@link GroupBy} command.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GroupByTest {

    /**
     * Test method for {@link GroupBy#GroupBy}.
     */
    @Test
    public void testGroupByMinimal() {
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.keys(keys);

        final GroupBy g = builder.build();
        assertNull(g.getKeyFunction());
        assertEquals(keys, g.getKeys());
        assertNull(g.getFinalizeFunction());
        assertNull(g.getInitialValue());
        assertNull(g.getQuery());
        assertNull(g.getReduceFunction());
    }

    /**
     * Test method for {@link GroupBy#GroupBy}.
     */
    @Test
    public void testGroupByNoKeysNoKeyFunction() {

        final GroupBy.Builder builder = GroupBy.builder();

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a GroupBy command without a query.",
                built);
    }

    /**
     * Test method for {@link GroupBy#GroupBy}.
     */
    @Test
    public void testGroupByWithFunction() {
        final Document initial = BuilderFactory.start().build();
        final Document query = BuilderFactory.start().addBoolean("foo", true)
                .build();

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeyFunction("function f() {}");
        builder.setFinalizeFunction("finalize");
        builder.setInitialValue(initial);
        builder.setQuery(query);
        builder.setReduceFunction("reduce");

        final GroupBy g = builder.build();
        assertEquals("function f() {}", g.getKeyFunction());
        assertEquals(0, g.getKeys().size());
        assertEquals("finalize", g.getFinalizeFunction());
        assertSame(initial, g.getInitialValue());
        assertSame(query, g.getQuery());
        assertEquals("reduce", g.getReduceFunction());
    }

    /**
     * Test method for {@link GroupBy#GroupBy}.
     */
    @Test
    public void testGroupByWithKeys() {
        final Document initial = BuilderFactory.start().build();
        final Document query = BuilderFactory.start().addBoolean("foo", true)
                .build();
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.keys(keys).finalize("finalize").initialValue(initial)
        .query(query).reduce("reduce");

        GroupBy g = builder.build();
        assertNull(g.getKeyFunction());
        assertEquals(keys, g.getKeys());
        assertEquals("finalize", g.getFinalizeFunction());
        assertSame(initial, g.getInitialValue());
        assertSame(query, g.getQuery());
        assertEquals("reduce", g.getReduceFunction());

        // Zap the keys and switch to a function
        builder.keys(null).key("function f() {}");

        g = builder.build();
        assertEquals("function f() {}", g.getKeyFunction());
        assertEquals(0, g.getKeys().size());
        assertEquals("finalize", g.getFinalizeFunction());
        assertSame(initial, g.getInitialValue());
        assertSame(query, g.getQuery());
        assertEquals("reduce", g.getReduceFunction());

        // Reset to nothing.
        boolean built = false;
        try {
            builder.reset().build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a GroupBy command without a query.",
                built);
        g = builder.keys(keys).build();
        assertNull(g.getKeyFunction());
        assertEquals(keys, g.getKeys());
        assertNull(g.getFinalizeFunction());
        assertNull(g.getInitialValue());
        assertNull(g.getQuery());
        assertNull(g.getReduceFunction());
    }

    /**
     * Test method for {@link GroupBy#GroupBy}.
     */
    @Test
    public void testGroupByWithReadPreference() {
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(keys);
        builder.readPreference(ReadPreference.SECONDARY);

        final GroupBy g = builder.build();
        assertNull(g.getKeyFunction());
        assertEquals(keys, g.getKeys());
        assertNull(g.getFinalizeFunction());
        assertNull(g.getInitialValue());
        assertNull(g.getQuery());
        assertNull(g.getReduceFunction());
        assertSame(ReadPreference.SECONDARY, g.getReadPreference());
    }

    /**
     * Test method for {@link GroupBy.Builder#setMaximumTimeMilliseconds(long)}
     * .
     */
    @Test
    public void testMaximumTimeMillisecondsDefault() {
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder b = GroupBy.builder();
        b.keys(keys);

        final GroupBy command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(0L));
    }

    /**
     * Test method for {@link GroupBy.Builder#setMaximumTimeMilliseconds(long)}
     * .
     */
    @Test
    public void testMaximumTimeMillisecondsViaFluent() {
        final Random random = new Random(System.currentTimeMillis());
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder b = GroupBy.builder();
        b.keys(keys);

        final long value = random.nextLong();
        b.maximumTime(value, TimeUnit.MILLISECONDS);

        final GroupBy command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for {@link GroupBy.Builder#setMaximumTimeMilliseconds(long)}
     * .
     */
    @Test
    public void testMaximumTimeMillisecondsViaSetter() {
        final Random random = new Random(System.currentTimeMillis());
        final Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");

        final GroupBy.Builder b = GroupBy.builder();
        b.keys(keys);

        final long value = random.nextLong();
        b.setMaximumTimeMilliseconds(value);

        final GroupBy command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }
}
