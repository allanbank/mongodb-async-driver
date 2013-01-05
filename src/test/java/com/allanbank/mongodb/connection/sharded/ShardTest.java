/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * ShardTest provides tests for the {@link Shard} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardTest {

    /**
     * Test method for {@link Shard#Shard(String)} .
     */
    @Test
    public void testShard() {
        String name = "foo";
        Shard s = new Shard(name);
        assertSame(name, s.toString());
        assertSame(name, s.getShardId());
    }

    /**
     * Test method for {@link Shard#Shard(String)} .
     */
    @Test(expected = IllegalArgumentException.class)
    public void testShardWithNullThrows() {
        new Shard(null).hashCode();
    }

    /**
     * Test method for {@link Shard#equals(Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Shard> objs1 = new ArrayList<Shard>();
        final List<Shard> objs2 = new ArrayList<Shard>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            objs1.add(new Shard(name));
            objs2.add(new Shard(name));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Shard obj1 = objs1.get(i);
            Shard obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }
}
