/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static junit.framework.Assert.assertEquals;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
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
 * StringElementTest provides tests for the {@link StringElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringElementTest {

    /**
     * Test method for
     * {@link StringElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final StringElement element = new StringElement("foo", "string");

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitString(eq("foo"), eq("string"));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link StringElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2",
                null)) {
            for (int i = 0; i < 10; ++i) {
                final String value = "" + random.nextLong();
                objs1.add(new StringElement(name, value));
                objs2.add(new StringElement(name, value));
            }
            objs1.add(new StringElement(name, null));
            objs2.add(new StringElement(name, null));
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
     * Test method for {@link StringElement#getValue()}.
     */
    @Test
    public void testGetValue() {
        final StringElement element = new StringElement("foo", "string");

        assertEquals("string", element.getValue());
    }

    /**
     * Test method for
     * {@link StringElement#StringElement(java.lang.String, String)} .
     */
    @Test
    public void testStringElement() {
        final StringElement element = new StringElement("foo", "string");

        assertEquals("foo", element.getName());
        assertEquals("string", element.getValue());
        assertEquals(ElementType.STRING, element.getType());
    }

    /**
     * Test method for {@link StringElement#toString()}.
     */
    @Test
    public void testToString() {
        final StringElement element = new StringElement("foo", "string");

        assertEquals("\"foo\" : \"string\"", element.toString());
    }

    /**
     * Test method for {@link StringElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        StringElement element = new StringElement("foo", "string");

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals("string", element.getValue());
        assertEquals(ElementType.STRING, element.getType());
    }
}
