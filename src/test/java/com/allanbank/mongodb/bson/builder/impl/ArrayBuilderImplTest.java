/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;

/**
 * ArrayBuilderImplTest provides tests for the {@link ArrayBuilderImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayBuilderImplTest {

    /**
     * Test method for the {@link ArrayBuilderImpl#add(byte[])}.
     */
    @Test
    public void testAddByteArray() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.add(new byte[] { 1 });
        assertEquals(new BinaryElement("0", new byte[] { 1 }),
                builder.build()[0]);

        builder.reset();
        builder.add((byte[]) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Date)}.
     */
    @Test
    public void testAddDate() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final Date d = new Date();
        builder.add(d);
        assertEquals(new TimestampElement("0", d.getTime()), builder.build()[0]);

        builder.reset();
        builder.add((Date) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(DocumentAssignable)}.
     */
    @Test
    public void testAddDocumentAssignable() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add((DocumentAssignable) d);
        assertEquals(new DocumentElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((DocumentAssignable) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(ElementAssignable)}.
     */
    @Test
    public void testAddElementAssignable() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final DocumentElement d = new DocumentElement("g");
        builder.add((ElementAssignable) d);
        assertEquals(new DocumentElement("0"), builder.build()[0]);

        builder.reset();
        try {
            builder.add((ElementAssignable) null);
            fail("Should have thrown an IllegalArgumentException.");
        }
        catch (final IllegalArgumentException good) {
            // Good.
        }
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(ObjectId)}.
     */
    @Test
    public void testAddObjectId() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final ObjectId d = new ObjectId();
        builder.add(d);
        assertEquals(new ObjectIdElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((ObjectId) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(Pattern)}.
     */
    @Test
    public void testAddPattern() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final Pattern d = Pattern.compile(".*");
        builder.add(d);
        assertEquals(new RegularExpressionElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((Pattern) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for the {@link ArrayBuilderImpl#add(String)}.
     */
    @Test
    public void testAddString() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        final String d = ".*";
        builder.add(d);
        assertEquals(new StringElement("0", d), builder.build()[0]);

        builder.reset();
        builder.add((Pattern) null);
        assertEquals(new NullElement("0"), builder.build()[0]);
    }

    /**
     * Test method for {@link ArrayBuilderImpl#pushArray()}.
     */
    @Test
    public void testPushArray() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.pushArray();

        final ArrayElement element = builder.build("foo");
        assertTrue(element.getEntries().size() == 1);
        assertTrue(element.getEntries().get(0) instanceof ArrayElement);
        assertTrue(((ArrayElement) element.getEntries().get(0)).getEntries()
                .size() == 0);
    }

    /**
     * Test method for {@link ArrayBuilderImpl#reset()}.
     */
    @Test
    public void testReset() {
        final ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.pushArray();

        ArrayElement element = builder.build("foo");
        assertTrue(element.getEntries().size() == 1);
        assertTrue(element.getEntries().get(0) instanceof ArrayElement);
        assertTrue(((ArrayElement) element.getEntries().get(0)).getEntries()
                .size() == 0);

        assertSame(builder, builder.reset());

        element = builder.build("foo");
        assertTrue(element.getEntries().size() == 0);
    }

}
