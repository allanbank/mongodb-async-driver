/*
 * #%L
 * VersionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
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
     * Test method for {@link Version#earlier}.
     */
    @Test
    public void testEarlier() {
        final String[] order = new String[] { "1", "1.2", "1.2.3", "2.1.1",
                "2.1.1-S", "2.1.1-T" };

        assertThat(Version.earlier(null, null), nullValue());

        for (int i = 0; i < order.length; ++i) {
            for (int j = i; j < order.length; ++j) {
                final Version ith = Version.parse(order[i]);
                final Version jth = Version.parse(order[j]);
                if (i == j) {
                    // Bias to lhs.
                    assertThat(ith + " == " + jth, Version.earlier(ith, jth),
                            sameInstance(ith));
                    assertThat(jth + " == " + ith, Version.earlier(jth, ith),
                            sameInstance(jth));
                }
                else {
                    assertThat(ith + " < " + jth, Version.earlier(ith, jth),
                            sameInstance(ith));
                    assertThat(jth + " > " + ith, Version.earlier(jth, ith),
                            sameInstance(ith));
                }

                // Unknown is the "highest" version. This ensures that version
                // check without a version for the server still go to the
                // server.
                assertThat(Version.earlier(ith, Version.UNKNOWN),
                        sameInstance(ith));
                assertThat(Version.earlier(Version.UNKNOWN, ith),
                        sameInstance(ith));

                // Null value returns non-null.
                assertThat(Version.earlier(ith, null), sameInstance(ith));
                assertThat(Version.earlier(null, ith), sameInstance(ith));
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
     * Test method for {@link Version#forWireVersion(int)}.
     */
    @Test
    public void testForWireVersion() {
        assertThat(Version.forWireVersion(Integer.MIN_VALUE), nullValue());
        assertThat(Version.forWireVersion(-1), nullValue());

        assertThat(Version.forWireVersion(0), is(Version.VERSION_2_4));
        assertThat(Version.forWireVersion(1), is(Version.VERSION_2_5_2));
        assertThat(Version.forWireVersion(2), is(Version.VERSION_2_5_4));

        assertThat(Version.forWireVersion(3), is(Version.VERSION_2_5_4));
        assertThat(Version.forWireVersion(Integer.MAX_VALUE),
                is(Version.VERSION_2_5_4));
    }

    /**
     * Test method for {@link Version#later}.
     */
    @Test
    public void testLater() {
        final String[] order = new String[] { "1", "1.2", "1.2.3", "2.1.1",
                "2.1.1-S", "2.1.1-T" };

        assertThat(Version.later(null, null), nullValue());

        for (int i = 0; i < order.length; ++i) {
            for (int j = i; j < order.length; ++j) {
                final Version ith = Version.parse(order[i]);
                final Version jth = Version.parse(order[j]);
                if (i == j) {
                    // Bias to lhs.
                    assertThat(ith + " == " + jth, Version.later(ith, jth),
                            sameInstance(ith));
                    assertThat(jth + " == " + ith, Version.later(jth, ith),
                            sameInstance(jth));
                }
                else {
                    assertThat(ith + " < " + jth, Version.later(ith, jth),
                            sameInstance(jth));
                    assertThat(jth + " > " + ith, Version.later(jth, ith),
                            sameInstance(jth));
                }

                // Unknown is the "highest" version. This ensures that version
                // check without a version for the server still go to the
                // server.
                assertThat(Version.later(ith, Version.UNKNOWN),
                        sameInstance(Version.UNKNOWN));
                assertThat(Version.later(Version.UNKNOWN, ith),
                        sameInstance(Version.UNKNOWN));

                // Null value returns non-null.
                assertThat(Version.later(ith, null), sameInstance(ith));
                assertThat(Version.later(null, ith), sameInstance(ith));
            }
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
