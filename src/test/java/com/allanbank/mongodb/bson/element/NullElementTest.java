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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * NullElementTest provides tests for the {@link NullElement} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NullElementTest {

    /**
     * Test method for
     * {@link NullElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final NullElement element = new NullElement("foo");

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitNull(eq("foo"));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link NullElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final NullElement a = new NullElement("a");
        final NullElement b = new NullElement("b");
        final Element other = new MaxKeyElement("a");

        assertEquals(0, a.compareTo(a));

        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);

        assertTrue(a.compareTo(other) < 0);
        assertTrue(other.compareTo(a) > 0);
    }

    /**
     * Test method for {@link NullElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            objs1.add(new NullElement(name));
            objs2.add(new NullElement(name));
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
     * Test method for {@link NullElement#NullElement(String)} .
     */
    @Test
    public void testNullElement() {
        final NullElement element = new NullElement("foo");

        assertEquals("foo", element.getName());
        assertEquals(ElementType.NULL, element.getType());
    }

    /**
     * Test method for {@link NullElement#NullElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new NullElement(null);
    }

    /**
     * Test method for {@link NullElement#toString()}.
     */
    @Test
    public void testToString() {
        final NullElement element = new NullElement("foo");

        assertEquals("foo : null", element.toString());
    }

    /**
     * Test method for {@link NullElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final NullElement element = new NullElement("foo");

        assertNull(element.getValueAsObject());
    }

    /**
     * Test method for {@link NullElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final NullElement element = new NullElement("foo");

        assertEquals("null", element.getValueAsString());
    }

    /**
     * Test method for {@link NullElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        NullElement element = new NullElement("foo");

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(ElementType.NULL, element.getType());
    }

    /**
     * Test method for {@link NullElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final NullElement element = new NullElement("foo");

        assertSame(element, element.withName("foo"));
    }
}
