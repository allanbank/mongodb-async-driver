/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
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
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * ReadPreferenceTest provides tests for the {@link ReadPreference} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReadPreferenceTest {

    /** A few test tag documents for use in tests. */
    private List<Document[]> myTestTags;

    /**
     * Create test tag document arrays.
     */
    @Before
    public void setUp() {
        myTestTags = Arrays.asList(
                new Document[0],
                new Document[] { BuilderFactory.start().addInteger("a", 1)
                        .build() },
                        new Document[] {
                        BuilderFactory.start().addInteger("a", 1)
                        .addInteger("b", 2).build(),
                        BuilderFactory.start().addInteger("b", 1).build() });
    }

    /**
     * Cleanup after the test.
     */
    @After
    public void tearDown() {
        myTestTags = null;
    }

    /**
     * Test method for {@link ReadPreference#closest(DocumentAssignable[])} .
     */
    @Test
    public void testClosest() {
        ReadPreference preference = ReadPreference.closest();

        assertEquals(ReadPreference.Mode.NEAREST, preference.getMode());
        assertSame(Collections.EMPTY_LIST, preference.getTagMatchingDocuments());
        assertEquals("nearest", preference.getMode().getToken());

        for (final Document[] tags : myTestTags) {
            preference = ReadPreference.closest(tags);

            assertEquals(ReadPreference.Mode.NEAREST, preference.getMode());
            assertEquals(Arrays.asList(tags),
                    preference.getTagMatchingDocuments());
        }
    }

    /**
     * Test method for {@link ReadPreference#equals} and
     * {@link ReadPreference#hashCode}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<ReadPreference> objs1 = new ArrayList<ReadPreference>();
        final List<ReadPreference> objs2 = new ArrayList<ReadPreference>();

        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            for (final Document[] tags : myTestTags) {
                objs1.add(new ReadPreference(mode, tags));
                objs2.add(new ReadPreference(mode, tags));
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final ReadPreference obj1 = objs1.get(i);
            ReadPreference obj2 = objs2.get(i);

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
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesEmpty() {
        final Document t = BuilderFactory.start().build();

        Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build(),
                BuilderFactory.start().build() }; // Empty document matches all.
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }

        tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesEmptyTags() {
        final Document t = BuilderFactory.start().addInteger("a", 1).build();

        final Document[] tags = new Document[] {}; // Empty document matches
        // all.
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesExactElements() {
        final Document t = BuilderFactory.start().addInteger("c", 1).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesExactElementsFirst() {
        final Document t = BuilderFactory.start().addInteger("a", 1)
                .addInteger("b", 2).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesFuzzyNumericDouble() {
        final Document t = BuilderFactory.start().addDouble("c", 1.0).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesFuzzyNumericLong() {
        final Document t = BuilderFactory.start().addLong("c", 1).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesFuzzyNumericLongNoMatch() {
        final Document t = BuilderFactory.start().addLong("c", 2).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesFuzzyString() {
        final Document t = BuilderFactory.start().addString("c", "1.0").build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesFuzzyStringTag() {
        final Document t = BuilderFactory.start().addDouble("c", 1.0).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addString("c", "1").build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesNotOverlapping() {
        final Document t = BuilderFactory.start().addInteger("b", 2)
                .addInteger("d", 3).build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesNull() {
        Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build(),
                BuilderFactory.start().build() }; // Empty document matches all.
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(null));
        }

        tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(null));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesWithMoreElements() {
        final Document t = BuilderFactory.start().addInteger("c", 1)
                .addString("d", "ab").build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertTrue(prefs.matches(t));
        }
    }

    /**
     * Test method for {@link ReadPreference#matches(Document)} .
     */
    @Test
    public void testMatchesWrongType() {
        final Document t = BuilderFactory.start().addString("c", "1").build();

        final Document[] tags = new Document[] {
                BuilderFactory.start().addInteger("a", 1).addInteger("b", 2)
                .build(),
                BuilderFactory.start().addInteger("c", 1).build() };
        for (final ReadPreference.Mode mode : ReadPreference.Mode.values()) {
            final ReadPreference prefs = new ReadPreference(mode, tags);
            assertFalse(prefs.matches(t));
        }
    }

    /**
     * Test method for
     * {@link ReadPreference#preferPrimary(DocumentAssignable[])} .
     */
    @Test
    public void testPreferPrimary() {
        ReadPreference preference = ReadPreference.preferPrimary();

        assertEquals(ReadPreference.Mode.PRIMARY_PREFERRED,
                preference.getMode());
        assertSame(Collections.EMPTY_LIST, preference.getTagMatchingDocuments());

        for (final Document[] tags : myTestTags) {
            preference = ReadPreference.preferPrimary(tags);

            assertEquals(ReadPreference.Mode.PRIMARY_PREFERRED,
                    preference.getMode());
            assertEquals(Arrays.asList(tags),
                    preference.getTagMatchingDocuments());
        }
    }

    /**
     * Test method for
     * {@link ReadPreference#preferSecondary(DocumentAssignable[])} .
     */
    @Test
    public void testPreferSecondary() {
        ReadPreference preference = ReadPreference.preferSecondary();

        assertEquals(ReadPreference.Mode.SECONDARY_PREFERRED,
                preference.getMode());
        assertSame(Collections.EMPTY_LIST, preference.getTagMatchingDocuments());

        for (final Document[] tags : myTestTags) {
            preference = ReadPreference.preferSecondary(tags);

            assertEquals(ReadPreference.Mode.SECONDARY_PREFERRED,
                    preference.getMode());
            assertEquals(Arrays.asList(tags),
                    preference.getTagMatchingDocuments());
        }
    }

    /**
     * Test method for {@link ReadPreference#primary()}.
     */
    @Test
    public void testPrimary() {
        final ReadPreference preference = ReadPreference.primary();

        assertEquals(ReadPreference.Mode.PRIMARY_ONLY, preference.getMode());
        assertSame(Collections.EMPTY_LIST, preference.getTagMatchingDocuments());
    }

    /**
     * Test method for {@link ReadPreference#readResolve} .
     *
     * @throws IOException
     *             On a failure.
     * @throws ClassNotFoundException
     *             On a failure.
     */
    @Test
    public void testReadResolve() throws IOException, ClassNotFoundException {
        for (final ReadPreference rp : Arrays.asList(ReadPreference.CLOSEST,
                ReadPreference.PREFER_PRIMARY, ReadPreference.PREFER_SECONDARY,
                ReadPreference.PRIMARY, ReadPreference.SECONDARY)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final ObjectOutputStream oout = new ObjectOutputStream(out);
            oout.writeObject(rp);
            oout.close();

            final ByteArrayInputStream in = new ByteArrayInputStream(
                    out.toByteArray());
            final ObjectInputStream oin = new ObjectInputStream(in);

            assertSame(rp, oin.readObject());
        }

        final ReadPreference rp = ReadPreference.closest(BuilderFactory.start()
                .build());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(out);
        oout.writeObject(rp);
        oout.close();

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final ObjectInputStream oin = new ObjectInputStream(in);

        final Object read = oin.readObject();
        assertEquals(rp, read);
        assertFalse(rp == read);
    }

    /**
     * Test method for {@link ReadPreference#secondary(DocumentAssignable[])} .
     */
    @Test
    public void testSecondary() {
        ReadPreference preference = ReadPreference.secondary();

        assertEquals(ReadPreference.Mode.SECONDARY_ONLY, preference.getMode());
        assertSame(Collections.EMPTY_LIST, preference.getTagMatchingDocuments());

        for (final Document[] tags : myTestTags) {
            preference = ReadPreference.secondary(tags);

            assertEquals(ReadPreference.Mode.SECONDARY_ONLY,
                    preference.getMode());
            assertEquals(Arrays.asList(tags),
                    preference.getTagMatchingDocuments());
        }
    }

    /**
     * Test method for {@link ReadPreference#toString()} .
     */
    @Test
    public void testToString() {
        assertEquals("NEAREST", ReadPreference.closest().toString());
        assertEquals("PRIMARY_PREFERRED", ReadPreference.preferPrimary()
                .toString());
        assertEquals("SECONDARY_PREFERRED", ReadPreference.preferSecondary()
                .toString());
        assertEquals("PRIMARY_ONLY", ReadPreference.primary().toString());
        assertEquals("SECONDARY_ONLY", ReadPreference.secondary().toString());
        assertEquals("SERVER[localhost:1111]",
                ReadPreference.server("localhost:1111").toString());

        final DocumentBuilder b1 = BuilderFactory.start();
        b1.addString("f", "3");

        final DocumentBuilder b2 = BuilderFactory.start();
        b2.addString("g", "2");
        b2.addString("e", "1");

        assertEquals("SECONDARY_ONLY[{f : '3'}]",
                ReadPreference.secondary(b1.build()).toString());

        assertEquals("SECONDARY_ONLY[{f : '3'}, {g : '2', e : '1'}]",
                ReadPreference.secondary(b1.build(), b2.build()).toString());

    }

}
