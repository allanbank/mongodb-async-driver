/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;

/**
 * VersionTest provides tests for the {@link Version} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VersionTest {

    /**
     * Test method for {@link Version#VERSION}.
     */
    @Test
    public void testAutoVersion() {
        assertThat(Version.VERSION, notNullValue());
    }

    /**
     * Test method for {@link Version#compareTo(com.allanbank.mongodb.Version)}.
     */
    @Test
    public void testCompareTo() {
        final String[] order = new String[] { "1", "1.2", "1.2.3", "2.1.1",
                "2.1.1-S", "2.1.1-T" };

        for (int i = 0; i < order.length; ++i) {
            for (int j = i; j < order.length; ++j) {
                final Version ith = Version.parse(order[i]);
                final Version jth = Version.parse(order[j]);
                if (i == j) {
                    assertThat(ith + " == " + jth, ith.compareTo(jth), is(0));
                    assertThat(jth + " == " + ith, jth.compareTo(ith), is(0));
                }
                else {
                    assertThat(ith + " < " + jth, ith.compareTo(jth),
                            lessThan(0));
                    assertThat(jth + " > " + ith, jth.compareTo(ith),
                            greaterThan(0));
                }

                // Unknown is the "highest" version. This ensures that version
                // check without a version for the server still go to the
                // server.
                assertThat(ith.compareTo(Version.UNKNOWN), lessThan(0));
                assertThat(Version.UNKNOWN.compareTo(ith), greaterThan(0));
            }
        }
    }

    /**
     * Test method for {@link Version#equals} and {@link Version#hashCode}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<Version> objs1 = new ArrayList<Version>();
        final List<Version> objs2 = new ArrayList<Version>();

        final String[] versions = new String[] { "1", "1.2", "1.2.3",
                "1-SNAPSHOT", "1.2-SNAPSHOT", "1.2.3-SNAPSHOT", "",
                String.valueOf(Integer.MAX_VALUE),
                String.valueOf(Integer.MIN_VALUE) };

        for (final String version : versions) {
            objs1.add(Version.parse(version));
            objs2.add(Version.parse(version));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Version obj1 = objs1.get(i);
            Version obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
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

    /**
     * Test method for {@link Version#parse(java.util.List)}.
     */
    @Test
    public void testParseListOfNumericElement() {

        final List<NumericElement> items = new ArrayList<NumericElement>();
        items.add(new IntegerElement("g", 1));
        items.add(new LongElement("g", 2));
        items.add(new DoubleElement("g", 3));

        assertThat(Version.parse(items), is(Version.parse("1.2.3")));
    }

    /**
     * Test method for {@link Version#toString()}.
     */
    @Test
    public void testToString() {
        assertThat(Version.parse("1.2.3.4.5").toString(), is("1.2.3.4.5"));
        assertThat(Version.parse("1.2.3.4.5-FOO").toString(),
                is("1.2.3.4.5-FOO"));
        assertThat(Version.parse("1-FOO").toString(), is("1-FOO"));
        assertThat(Version.UNKNOWN.toString(), is("UNKNOWN"));
    }

}
