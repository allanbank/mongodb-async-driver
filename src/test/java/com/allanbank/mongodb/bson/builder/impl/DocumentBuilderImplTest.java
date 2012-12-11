/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * DocumentBuilderImplTest provides tests for a {@link DocumentBuilderImpl}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentBuilderImplTest {

    /**
     * Test method for the {@link DocumentBuilderImpl#add(String, byte[])}.
     */
    @Test
    public void testAddStringByteArray() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        builder.add("f", new byte[] { 1 });
        assertEquals(
                new RootDocument(new BinaryElement("f", new byte[] { 1 })),
                builder.build());

        builder.reset();
        builder.add("f", (byte[]) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, new byte[] { 1 });
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link DocumentBuilderImpl#add(String, Date)}.
     */
    @Test
    public void testAddStringDate() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        final Date d = new Date();
        builder.add("f", d);
        assertEquals(new RootDocument(new TimestampElement("f", d.getTime())),
                builder.build());

        builder.reset();
        builder.add("f", (Date) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, d);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the
     * {@link DocumentBuilderImpl#add(String, DocumentAssignable)}.
     */
    @Test
    public void testAddStringDocumentAssignable() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add("f", d);
        assertEquals(new RootDocument(new DocumentElement("f", d)),
                builder.build());

        builder.reset();
        builder.add("f", (DocumentAssignable) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, d);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link DocumentBuilderImpl#add(String, ObjectId)}.
     */
    @Test
    public void testAddStringObjectId() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        final ObjectId d = new ObjectId();
        builder.add("f", d);
        assertEquals(new RootDocument(new ObjectIdElement("f", d)),
                builder.build());

        builder.reset();
        builder.add("f", (ObjectId) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, d);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link DocumentBuilderImpl#add(String, Pattern)}.
     */
    @Test
    public void testAddStringPattern() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        final Pattern d = Pattern.compile(".*");
        builder.add("f", d);
        assertEquals(new RootDocument(new RegularExpressionElement("f", d)),
                builder.build());

        builder.reset();
        builder.add("f", (Pattern) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, d);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link DocumentBuilderImpl#add(String, String)}.
     */
    @Test
    public void testAddStringString() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        final String d = ".*";
        builder.add("f", d);
        assertEquals(new RootDocument(new StringElement("f", d)),
                builder.build());

        builder.reset();
        builder.add("f", (Pattern) null);
        assertEquals(new RootDocument(new NullElement("f")), builder.build());

        builder.reset();
        try {
            builder.add((String) null, d);
            fail("Should have thrown a IllegalArguementException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link DocumentBuilderImpl#build()} .
     */
    @Test
    public void testGetWithoutUniqueNames() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();
        builder.addBoolean("bool", true);
        builder.addBoolean("bool", true);

        try {
            builder.build();
            fail("Should not be able to create a document without unique names.");
        }
        catch (final AssertionError error) {
            // good.
        }
    }

    /**
     * Test method for {@link DocumentBuilderImpl#reset()}.
     */
    @Test
    public void testReset() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        builder.pushArray("a");

        Document element = builder.build();

        Iterator<Element> iter = element.iterator();
        assertTrue(iter.hasNext());
        assertTrue(iter.next() instanceof ArrayElement);
        assertFalse(iter.hasNext());

        assertSame(builder, builder.reset());

        element = builder.build();
        iter = element.iterator();
        assertFalse(iter.hasNext());
    }
}
