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
 * IntegerElementTest provides tests for the {@link IntegerElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IntegerElementTest {

    /**
     * Test method for
     * {@link IntegerElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitInteger(eq("foo"), eq(1010101));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link IntegerElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final IntegerElement a1 = new IntegerElement("a", 1);
        final IntegerElement a11 = new IntegerElement("a", 11);
        final IntegerElement b1 = new IntegerElement("b", 1);

        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for {@link IntegerElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final int value = random.nextInt();
                objs1.add(new IntegerElement(name, value));
                objs2.add(new IntegerElement(name, value));
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
     * Test method for {@link IntegerElement#getDoubleValue()}.
     */
    @Test
    public void testGetDoubleValue() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals(1010101.0, element.getDoubleValue(), 0.001);
    }

    /**
     * Test method for {@link IntegerElement#getIntValue()}.
     */
    @Test
    public void testGetIntValue() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals(1010101, element.getIntValue());
    }

    /**
     * Test method for {@link IntegerElement#getLongValue()} .
     */
    @Test
    public void testGetLongValue() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals(1010101L, element.getLongValue());
    }

    /**
     * Test method for {@link IntegerElement#getValue()}.
     */
    @Test
    public void testGetValue() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals(1010101L, element.getValue());
    }

    /**
     * Test method for {@link IntegerElement#IntegerElement(String, int)} .
     */
    @Test
    public void testIntegerElement() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals("foo", element.getName());
        assertEquals(1010101, element.getValue(), 0.0001);
        assertEquals(ElementType.INTEGER, element.getType());
    }

    /**
     * Test method for {@link IntegerElement#IntegerElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new IntegerElement(null, 1);
    }

    /**
     * Test method for {@link IntegerElement#toString()}.
     */
    @Test
    public void testToString() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals("foo : 1010101", element.toString());
    }

    /**
     * Test method for {@link IntegerElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals(Integer.valueOf(element.getIntValue()),
                element.getValueAsObject());
    }

    /**
     * Test method for {@link IntegerElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertEquals("1010101", element.getValueAsString());
    }

    /**
     * Test method for {@link IntegerElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        IntegerElement element = new IntegerElement("foo", 1010101);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(1010101, element.getValue(), 0.0001);
        assertEquals(ElementType.INTEGER, element.getType());
    }

    /**
     * Test method for {@link IntegerElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final IntegerElement element = new IntegerElement("foo", 1010101);

        assertSame(element, element.withName("foo"));
    }
}
