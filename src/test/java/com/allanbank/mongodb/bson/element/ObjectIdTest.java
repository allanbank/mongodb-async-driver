/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * ObjectIdTest provides tests for the {@link ObjectId} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ObjectIdTest {

    /**
     * Test method for {@link ObjectId#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<ObjectId> objs1 = new ArrayList<ObjectId>();
        final List<ObjectId> objs2 = new ArrayList<ObjectId>();

        for (int i = 0; i < 10; ++i) {
            int time = random.nextInt();
            final long l = random.nextLong();
            objs1.add(new ObjectId(time, l));
            objs2.add(new ObjectId(time, l));

            time = random.nextInt();
            objs1.add(new ObjectId(time, l));
            objs2.add(new ObjectId(time, l));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final ObjectId obj1 = objs1.get(i);
            ObjectId obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse("" + obj1 + " != " + obj2,
                        obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new MaxKeyElement("foo")));
        }
    }

    /**
     * Test method for {@link ObjectId#getMachineId()}.
     */
    @Test
    public void testGetMachineId() {
        final Random random = new Random(System.currentTimeMillis());
        final int time = random.nextInt();
        final long l = random.nextLong();
        final ObjectId id = new ObjectId(time, l);

        assertEquals(time, id.getTimestamp());
        assertEquals(l, id.getMachineId());
    }

    /**
     * Test method for {@link ObjectId#toString()}.
     */
    @Test
    public void testToString() {
        final ObjectId id = new ObjectId(0x01020304, 0x0102030405060708L);

        assertEquals("ObjectId(010203040102030405060708)", id.toString());
    }

}
