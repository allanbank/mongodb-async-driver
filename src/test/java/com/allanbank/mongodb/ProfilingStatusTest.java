/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * ProfilingStatusTest provides tests for the {@link ProfilingStatus} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ProfilingStatusTest {

    /**
     * Test method for
     * {@link ProfilingStatus#compareTo(com.allanbank.mongodb.ProfilingStatus)}.
     */
    @Test
    public void testCompareTo() {
        assertTrue(ProfilingStatus.OFF.compareTo(ProfilingStatus.OFF) == 0);
        assertTrue(ProfilingStatus.OFF.compareTo(ProfilingStatus.ON) < 0);
        assertTrue(ProfilingStatus.OFF.compareTo(ProfilingStatus.slow(10)) < 0);
        assertTrue(ProfilingStatus.OFF.compareTo(ProfilingStatus.slow(100)) < 0);

        assertTrue(ProfilingStatus.ON.compareTo(ProfilingStatus.OFF) > 0);
        assertTrue(ProfilingStatus.ON.compareTo(ProfilingStatus.ON) == 0);
        assertTrue(ProfilingStatus.ON.compareTo(ProfilingStatus.slow(10)) > 0);
        assertTrue(ProfilingStatus.ON.compareTo(ProfilingStatus.slow(100)) > 0);

        ProfilingStatus s = ProfilingStatus.slow(10);
        assertTrue(s.compareTo(ProfilingStatus.OFF) > 0);
        assertTrue(s.compareTo(ProfilingStatus.ON) < 0);
        assertTrue(s.compareTo(ProfilingStatus.slow(10)) == 0);
        assertTrue(s.compareTo(ProfilingStatus.slow(100)) < 0);

        s = ProfilingStatus.slow(100);
        assertTrue(s.compareTo(ProfilingStatus.OFF) > 0);
        assertTrue(s.compareTo(ProfilingStatus.ON) < 0);
        assertTrue(s.compareTo(ProfilingStatus.slow(10)) > 0);
        assertTrue(s.compareTo(ProfilingStatus.slow(100)) == 0);
    }

    /**
     * Test method for {@link ProfilingStatus#slow(int)}.
     */
    @Test
    public void testConstruction() {
        assertSame(ProfilingStatus.Level.ALL, ProfilingStatus.ON.getLevel());
        assertEquals(ProfilingStatus.DEFAULT_SLOW_MS,
                ProfilingStatus.ON.getSlowMillisThreshold());

        assertSame(ProfilingStatus.Level.NONE, ProfilingStatus.OFF.getLevel());
        assertEquals(ProfilingStatus.DEFAULT_SLOW_MS,
                ProfilingStatus.OFF.getSlowMillisThreshold());

        ProfilingStatus s = ProfilingStatus.slow(10);
        assertSame(ProfilingStatus.Level.SLOW_ONLY, s.getLevel());
        assertEquals(10, s.getSlowMillisThreshold());

        s = ProfilingStatus.slow(10000);
        assertSame(ProfilingStatus.Level.SLOW_ONLY, s.getLevel());
        assertEquals(10000, s.getSlowMillisThreshold());
    }

    /**
     * Test method for {@link ProfilingStatus#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
        final List<ProfilingStatus> objs1 = new ArrayList<ProfilingStatus>();
        final List<ProfilingStatus> objs2 = new ArrayList<ProfilingStatus>();

        objs1.add(ProfilingStatus.ON);
        objs2.add(ProfilingStatus.ON);

        objs1.add(ProfilingStatus.OFF);
        objs2.add(ProfilingStatus.OFF);

        for (final int millis : new int[] { 1, 2, 3, 4, 10, 100, -1 }) {
            objs1.add(ProfilingStatus.slow(millis));
            objs2.add(ProfilingStatus.slow(millis));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final ProfilingStatus obj1 = objs1.get(i);
            ProfilingStatus obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse("" + obj1 + " " + obj2,
                        obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }

    /**
     * Test method for {@link ProfilingStatus#readResolve} .
     * 
     * @throws IOException
     *             On a failure.
     * @throws ClassNotFoundException
     *             On a failure.
     */
    @Test
    public void testReadResolve() throws IOException, ClassNotFoundException {
        for (final ProfilingStatus d : Arrays.asList(ProfilingStatus.ON,
                ProfilingStatus.OFF)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final ObjectOutputStream oout = new ObjectOutputStream(out);
            oout.writeObject(d);
            oout.close();

            final ByteArrayInputStream in = new ByteArrayInputStream(
                    out.toByteArray());
            final ObjectInputStream oin = new ObjectInputStream(in);

            assertSame(d, oin.readObject());
        }

        final ProfilingStatus d = ProfilingStatus.slow(100);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(out);
        oout.writeObject(d);
        oout.close();

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final ObjectInputStream oin = new ObjectInputStream(in);

        final Object read = oin.readObject();
        assertEquals(d, read);
        assertFalse(d == read);
    }

    /**
     * Test method for {@link ProfilingStatus#toString()}.
     */
    @Test
    public void testToString() {
        assertEquals("ALL", ProfilingStatus.Level.ALL.toString());
        assertEquals("NONE", ProfilingStatus.Level.NONE.toString());

        ProfilingStatus s = ProfilingStatus.slow(10);
        assertEquals("SLOW_ONLY(10 ms)", s.toString());

        s = ProfilingStatus.slow(10000);
        assertEquals("SLOW_ONLY(10000 ms)", s.toString());
    }

}
