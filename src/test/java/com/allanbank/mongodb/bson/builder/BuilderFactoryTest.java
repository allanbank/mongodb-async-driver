/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;

/**
 * BuilderFactoryTest provides tests for the {@link BuilderFactory} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuilderFactoryTest {

    /**
     * Test method for {@link BuilderFactory#a(Boolean...)}.
     */
    @Test
    public void testABooleanArray() {
        assertThat(BuilderFactory.a(true, false), is(new ArrayElement("",
                new BooleanElement("0", true), new BooleanElement("1", false))));
    }

    /**
     * Test method for {@link BuilderFactory#a(byte[][])}.
     */
    @Test
    public void testAByteArrayArray() {
        assertThat(BuilderFactory.a(new byte[1], new byte[2]),
                is(new ArrayElement("", new BinaryElement("0", new byte[1]),
                        new BinaryElement("1", new byte[2]))));
    }

    /**
     * Test method for {@link BuilderFactory#a(Date[])}.
     */
    @Test
    public void testADateArray() {
        final Date d1 = new Date();
        final Date d2 = new Date(d1.getTime() + 1000);
        assertThat(BuilderFactory.a(d1, d2), is(new ArrayElement("",
                new TimestampElement("0", d1.getTime()), new TimestampElement(
                        "1", d2.getTime()))));
    }

    /**
     * Test method for {@link BuilderFactory#a(DocumentAssignable[])} .
     */
    @Test
    public void testADocumentAssignableArray() {
        final DocumentAssignable d1 = BuilderFactory.start().add("a", 1);
        final DocumentAssignable d2 = BuilderFactory.start().add("b", 2);
        assertThat(BuilderFactory.a(d1, d2), is(new ArrayElement("",
                new DocumentElement("0", d1.asDocument()), new DocumentElement(
                        "1", d2.asDocument()))));
    }

    /**
     * Test method for {@link BuilderFactory#a(double...)}.
     */
    @Test
    public void testADoubleArray() {
        assertThat(BuilderFactory.a(1.2, 3.4), is(new ArrayElement("",
                new DoubleElement("0", 1.2), new DoubleElement("1", 3.4))));
    }

    /**
     * Test method for {@link BuilderFactory#a(ElementAssignable[])}.
     */
    @Test
    public void testAElementAssignableArray() {
        final ElementAssignable e1 = new IntegerElement("0", 1);
        final ElementAssignable e2 = new IntegerElement("0", 2);
        assertThat(BuilderFactory.a(e1, e2), is(new ArrayElement("",
                new IntegerElement("0", 1), new IntegerElement("0", 2))));
    }

    /**
     * Test method for {@link BuilderFactory#a(Integer...)}.
     */
    @Test
    public void testAIntArray() {
        assertThat(BuilderFactory.a(1, 2, 3, 4), is(new ArrayElement("",
                new IntegerElement("0", 1), new IntegerElement("0", 2),
                new IntegerElement("0", 3), new IntegerElement("1", 4))));
    }

    /**
     * Test method for {@link BuilderFactory#a(Long...)}.
     */
    @Test
    public void testALongArray() {
        assertThat(BuilderFactory.a(1L, 2L, 3L, 4L), is(new ArrayElement("",
                new LongElement("0", 1), new LongElement("0", 2),
                new LongElement("0", 3), new LongElement("1", 4))));
    }

    /**
     * Test method for {@link BuilderFactory#a(Object[])}.
     */
    @Test
    public void testAObjectArray() {
        assertThat(BuilderFactory.a(1, 2L, 3.3, "String"), is(new ArrayElement(
                "", new IntegerElement("0", 1), new LongElement("0", 2),
                new DoubleElement("0", 3.3), new StringElement("1", "String"))));
    }

    /**
     * Test method for {@link BuilderFactory#a(ObjectId[])}.
     */
    @Test
    public void testAObjectIdArray() {
        final ObjectId o1 = new ObjectId();
        final ObjectId o2 = new ObjectId();
        assertThat(BuilderFactory.a(o1, o2), is(new ArrayElement("",
                new ObjectIdElement("0", o1), new ObjectIdElement("0", o2))));
    }

    /**
     * Test method for {@link BuilderFactory#a(Pattern[])}.
     */
    @Test
    public void testAPatternArray() {
        final Pattern p1 = Pattern.compile("a");
        final Pattern p2 = Pattern.compile("b");
        assertThat(BuilderFactory.a(p1, p2), is(new ArrayElement("",
                new RegularExpressionElement("0", p1),
                new RegularExpressionElement("0", p2))));
    }

    /**
     * Test method for {@link BuilderFactory#a(String[])}.
     */
    @Test
    public void testAStringArray() {
        final String s1 = "a";
        final String s2 = "b";
        assertThat(BuilderFactory.a(s1, s2), is(new ArrayElement("",
                new StringElement("0", s1), new StringElement("0", s2))));
    }

    /**
     * Test method for {@link BuilderFactory#a(UUID[])}.
     */
    @Test
    public void testAUUIDArray() {
        final UUID u1 = UUID.randomUUID();
        final UUID u2 = UUID.randomUUID();
        assertThat(BuilderFactory.a(u1, u2), is(new ArrayElement("",
                new UuidElement("0", u1), new UuidElement("0", u2))));
    }

    /**
     * Test method for {@link BuilderFactory#d(Element[])}.
     */
    @Test
    public void testD() {
        final DocumentBuilder movie = BuilderFactory.d(BuilderFactory.e(
                "title", "Gone with the Wind"),
                BuilderFactory.e("directors", BuilderFactory.a(
                        "Victor Fleming", "George Cukor", "Sam Wood")),
                BuilderFactory.e("stars", BuilderFactory.a("Clark Gable",
                        "Vivien Leigh", "Thomas Mitchell")));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.add("title", "Gone with the Wind");
        final ArrayBuilder directorsArray = expected.pushArray("directors");
        directorsArray.add("Victor Fleming").add("George Cukor")
                .add("Sam Wood");
        final ArrayBuilder starsArray = expected.pushArray("stars");
        starsArray.add("Clark Gable").add("Vivien Leigh")
                .add("Thomas Mitchell");

        assertThat(movie.asDocument(), is(expected.asDocument()));

    }

    /**
     * Test method for {@link BuilderFactory#e(String, boolean)}.
     */
    @Test
    public void testEStringBoolean() {
        assertThat(BuilderFactory.e("e", false), is(new BooleanElement("e",
                false)));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, byte[])}.
     */
    @Test
    public void testEStringByteArray() {
        byte[] b = new byte[3];
        assertThat(BuilderFactory.e("e", b), is((Element) new BinaryElement(
                "e", b)));

        b = null;
        assertThat(BuilderFactory.e("e", b), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, Date)}.
     */
    @Test
    public void testEStringDate() {
        Date d = new Date();
        assertThat(BuilderFactory.e("e", d), is((Element) new TimestampElement(
                "e", d.getTime())));

        d = null;
        assertThat(BuilderFactory.e("e", d), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, DocumentAssignable)} .
     */
    @Test
    public void testEStringDocumentAssignable() {
        DocumentAssignable d = BuilderFactory.start().add("a", 1);
        assertThat(BuilderFactory.e("e", d), is((Element) new DocumentElement(
                "e", d.asDocument())));

        d = null;
        assertThat(BuilderFactory.e("e", d), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, double)}.
     */
    @Test
    public void testEStringDouble() {
        final double d = 1.2;
        assertThat(BuilderFactory.e("e", d), is((Element) new DoubleElement(
                "e", d)));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, ElementAssignable)} .
     */
    @Test
    public void testEStringElementAssignable() {
        ElementAssignable e = new IntegerElement("f", 1);
        assertThat(BuilderFactory.e("e", e), is(e.asElement().withName("e")));

        e = null;
        assertThat(BuilderFactory.e("e", e), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, int)}.
     */
    @Test
    public void testEStringInt() {
        final int i = 1;
        assertThat(BuilderFactory.e("e", i), is((Element) new IntegerElement(
                "e", i)));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, long)}.
     */
    @Test
    public void testEStringLong() {
        final long l = 1;
        assertThat(BuilderFactory.e("e", l), is((Element) new LongElement("e",
                l)));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, Object)}.
     */
    @Test
    public void testEStringObject() {
        Object e = "string";
        assertThat(BuilderFactory.e("e", e), is((Element) new StringElement(
                "e", e.toString())));

        e = null;
        assertThat(BuilderFactory.e("e", e), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, ObjectId)} .
     */
    @Test
    public void testEStringObjectId() {
        ObjectId o = new ObjectId();
        assertThat(BuilderFactory.e("e", o), is((Element) new ObjectIdElement(
                "e", o)));

        o = null;
        assertThat(BuilderFactory.e("e", o), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, Pattern)}.
     */
    @Test
    public void testEStringPattern() {
        Pattern p = Pattern.compile("a");
        assertThat(BuilderFactory.e("e", p),
                is((Element) new RegularExpressionElement("e", p)));

        p = null;
        assertThat(BuilderFactory.e("e", p), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, String)}.
     */
    @Test
    public void testEStringString() {
        String s = "z";
        assertThat(BuilderFactory.e("e", s), is((Element) new StringElement(
                "e", s)));

        s = null;
        assertThat(BuilderFactory.e("e", s), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#e(String, UUID)}.
     */
    @Test
    public void testEStringUUID() {
        UUID u = UUID.randomUUID();
        assertThat(BuilderFactory.e("e", u), is((Element) new UuidElement("e",
                u)));

        u = null;
        assertThat(BuilderFactory.e("e", u), is((Element) new NullElement("e")));
    }

    /**
     * Test method for {@link BuilderFactory#start()}.
     */
    @Test
    public void testStart() {
        assertNotNull(BuilderFactory.start());
        assertTrue(BuilderFactory.start() instanceof DocumentBuilderImpl);
    }

    /**
     * Test method for {@link BuilderFactory#startArray()}.
     */
    @Test
    public void testStartArray() {
        assertNotNull(BuilderFactory.startArray());
        assertTrue(BuilderFactory.startArray() instanceof ArrayBuilderImpl);
    }

    /**
     * Test method for {@link BuilderFactory#start(DocumentAssignable)}.
     */
    @Test
    public void testStartDocumentAssignable() {
        final DocumentBuilder seedBuilder = BuilderFactory.start().add("a", 1);
        final Document seed = seedBuilder.build();

        assertNotNull(BuilderFactory.start(seed));
        assertTrue(BuilderFactory.start(seed) instanceof DocumentBuilderImpl);
        assertEquals(seed.asDocument(), BuilderFactory.start(seed).build());
        assertEquals(seedBuilder.add("b", 2).asDocument(), BuilderFactory
                .start(seed).add("b", 2).build());
    }

    /**
     * Test method for {@link BuilderFactory#start(DocumentAssignable...)}.
     */
    @Test
    public void testStartDocumentAssignableArray() {
        final DocumentBuilder seedBuilder = BuilderFactory.start().add("a", 1);
        final Document seed1 = seedBuilder.build();
        final Document seed2 = seedBuilder.add("b", 2).build();

        final DocumentBuilder seeded = BuilderFactory.start(seed1, seed2);
        assertNotNull(seeded);
        assertTrue(seeded instanceof DocumentBuilderImpl);
        assertEquals(seed2, seeded.build()); // Only 1 a.
        assertEquals(seedBuilder.add("c", 3).asDocument(), seeded.add("c", 3)
                .build());
    }
}
