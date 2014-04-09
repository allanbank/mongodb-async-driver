/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * TimestampElementTest provides tests for the {@link TimestampElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TimestampElementTest {

    /**
     * Test method for
     * {@link TimestampElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitTimestamp(eq("foo"), eq(1010101L));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link TimestampElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final TimestampElement a1 = new TimestampElement("a", 1);
        final TimestampElement a11 = new TimestampElement("a", 11);
        final TimestampElement b1 = new TimestampElement("b", 1);

        final MongoTimestampElement i = new MongoTimestampElement("a", 2);

        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(i) < 0);
        assertTrue(i.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for {@link TimestampElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final long value = random.nextLong();
                objs1.add(new TimestampElement(name, value));
                objs2.add(new TimestampElement(name, value));
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Element obj1 = objs1.get(i);
            Element obj2 = objs2.get(i);

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
            assertFalse(obj1.equals(new MaxKeyElement(obj1.getName())));
        }
    }

    /**
     * Test method for {@link TimestampElement#getTime()}.
     */
    @Test
    public void testGetValue() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertEquals(1010101L, element.getTime());
    }

    /**
     * Test method for {@link TimestampElement#TimestampElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new TimestampElement(null, 1);
    }

    /**
     * Test method for
     * {@link TimestampElement#TimestampElement(java.lang.String, long)} .
     */
    @Test
    public void testTimestampElement() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertEquals("foo", element.getName());
        assertEquals(1010101, element.getTime(), 0.0001);
        assertEquals(ElementType.UTC_TIMESTAMP, element.getType());
    }

    /**
     * Test method for {@link TimestampElement#toString()}.
     */
    @Test
    public void testToString() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertEquals("foo : ISODate('1970-01-01T00:16:50.101+0000')",
                element.toString());
    }

    /**
     * Test method for {@link TimestampElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertEquals(new Date(1010101L), element.getValueAsObject());
    }

    /**
     * Test method for {@link TimestampElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertEquals("ISODate('1970-01-01T00:16:50.101+0000')",
                element.getValueAsString());
    }

    /**
     * Test method for {@link TimestampElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        TimestampElement element = new TimestampElement("foo", 1010101);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(1010101, element.getTime(), 0.0001);
        assertEquals(ElementType.UTC_TIMESTAMP, element.getType());
    }

    /**
     * Test method for {@link TimestampElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final TimestampElement element = new TimestampElement("foo", 1010101);

        assertSame(element, element.withName("foo"));
    }
}
