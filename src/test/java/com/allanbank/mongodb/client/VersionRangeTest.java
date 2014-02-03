/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.Version;

/**
 * VersionRangeTest provides tests for the {@link VersionRange} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VersionRangeTest {

    /**
     * Test method for {@link VersionRange#contains(Version)}.
     */
    @Test
    public void testContains() {
        final VersionRange range = VersionRange.range(Version.parse("1.1.1"),
                Version.parse("2.2.2"));

        assertThat(range.contains(Version.parse("1.1.0")), is(false));
        assertThat(range.contains(Version.parse("1.1.1")), is(true));
        assertThat(range.contains(Version.parse("2.0.0")), is(true));
        assertThat(range.contains(Version.parse("2.2.1")), is(true));
        assertThat(range.contains(Version.parse("2.2.2")), is(false));
        assertThat(range.contains(Version.parse("2.2.3")), is(false));
    }

    /**
     * Test method for {@link VersionRange#equals} and
     * {@link VersionRange#hashCode}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<VersionRange> objs1 = new ArrayList<VersionRange>();
        final List<VersionRange> objs2 = new ArrayList<VersionRange>();

        final String[] versions = new String[] { "1", "1.2", "1.2.3",
                "1-SNAPSHOT", "1.2-SNAPSHOT", "1.2.3-SNAPSHOT",
                String.valueOf(Integer.MAX_VALUE),
                String.valueOf(Integer.MIN_VALUE) };

        for (final String low : versions) {
            for (final String high : versions) {
                objs1.add(VersionRange.range(Version.parse(low),
                        Version.parse(high)));
                objs2.add(VersionRange.range(Version.parse(low),
                        Version.parse(high)));
            }
            objs1.add(VersionRange.minimum(Version.parse(low)));
            objs2.add(VersionRange.minimum(Version.parse(low)));

            objs1.add(VersionRange.maximum(Version.parse(low)));
            objs2.add(VersionRange.maximum(Version.parse(low)));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final VersionRange obj1 = objs1.get(i);
            VersionRange obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertThat(obj1, is(not(obj2)));
                assertThat(obj1.hashCode(), is(not(obj2.hashCode())));
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }

    /**
     * Test method for {@link VersionRange#maximum(Version)}.
     */
    @Test
    public void testMaximum() {
        final VersionRange range = VersionRange.maximum(Version.parse("1.1.1"));

        assertThat(range.getLowerBounds(), is(Version.VERSION_0));
        assertThat(range.getUpperBounds(), is(Version.parse("1.1.1")));
        assertThat(range.toString(), is("[0, 1.1.1)"));
    }

    /**
     * Test method for {@link VersionRange#minimum(Version)}.
     */
    @Test
    public void testMinimum() {
        final VersionRange range = VersionRange.minimum(Version.parse("1.1.1"));

        assertThat(range.getLowerBounds(), is(Version.parse("1.1.1")));
        assertThat(range.getUpperBounds(), is(Version.UNKNOWN));
        assertThat(range.toString(), is("[1.1.1, UNKNOWN)"));
    }

    /**
     * Test method for {@link VersionRange#range(Version, Version)} .
     */
    @Test
    public void testRange() {
        final VersionRange range = VersionRange.range(Version.parse("1.1.1"),
                Version.parse("2.2.2"));

        assertThat(range.getLowerBounds(), is(Version.parse("1.1.1")));
        assertThat(range.getUpperBounds(), is(Version.parse("2.2.2")));
        assertThat(range.toString(), is("[1.1.1, 2.2.2)"));
    }
}
