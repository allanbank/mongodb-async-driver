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
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * LongElementTest provides tests for the {@link LongElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LongElementTest {

    /**
     * Test method for
     * {@link LongElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final LongElement element = new LongElement("foo", 1010101);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitLong(eq("foo"), eq(1010101L));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link LongElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final LongElement a1 = new LongElement("a", 1);
        final LongElement a11 = new LongElement("a", 11);
        final LongElement b1 = new LongElement("b", 1);

        final IntegerElement i = new IntegerElement("a", 2);

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
     * Test method for {@link LongElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final long value = random.nextLong();
                objs1.add(new LongElement(name, value));
                objs2.add(new LongElement(name, value));
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
     * Test method for {@link LongElement#getDoubleValue()}.
     */
    @Test
    public void testGetDoubleValue() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals(1010101.0, element.getDoubleValue(), 0.001);
    }

    /**
     * Test method for {@link LongElement#getIntValue()}.
     */
    @Test
    public void testGetIntValue() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals(1010101, element.getIntValue());
    }

    /**
     * Test method for {@link LongElement#getLongValue()} .
     */
    @Test
    public void testGetLongValue() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals(1010101, element.getLongValue(), 0.0001);
    }

    /**
     * Test method for {@link LongElement#getValue()}.
     */
    @Test
    public void testGetValue() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals(1010101L, element.getValue());
    }

    /**
     * Test method for {@link LongElement#LongElement(java.lang.String, long)} .
     */
    @Test
    public void testLongElement() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals("foo", element.getName());
        assertEquals(1010101, element.getValue(), 0.0001);
        assertEquals(ElementType.LONG, element.getType());
    }

    /**
     * Test method for {@link LongElement#LongElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new LongElement(null, 1);
    }

    /**
     * Test method for {@link LongElement#toString()}.
     */
    @Test
    public void testToString() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals("foo : NumberLong('1010101')", element.toString());
    }

    /**
     * Test method for {@link LongElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals(Long.valueOf(element.getLongValue()),
                element.getValueAsObject());
    }

    /**
     * Test method for {@link LongElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final LongElement element = new LongElement("foo", 1010101);

        assertEquals("1010101", element.getValueAsString());
    }

    /**
     * Test method for {@link LongElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        LongElement element = new LongElement("foo", 1010101);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(1010101, element.getValue(), 0.0001);
        assertEquals(ElementType.LONG, element.getType());
    }

    /**
     * Test method for {@link LongElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final LongElement element = new LongElement("foo", 1010101);

        assertSame(element, element.withName("foo"));
    }
}
