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

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * MaxKeyElementTest provides tests for the {@link MaxKeyElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MaxKeyElementTest {

    /**
     * Test method for
     * {@link MaxKeyElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final MaxKeyElement element = new MaxKeyElement("foo");

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitMaxKey(eq("foo"));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link MaxKeyElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2",
                null)) {
            objs1.add(new MaxKeyElement(name));
            objs2.add(new MaxKeyElement(name));
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
            assertFalse(obj1.equals(new MinKeyElement(obj1.getName())));
        }
    }

    /**
     * Test method for {@link MaxKeyElement#MaxKeyElement(String)} .
     */
    @Test
    public void testMaxKeyElement() {
        final MaxKeyElement element = new MaxKeyElement("foo");

        assertEquals("foo", element.getName());
        assertEquals(ElementType.MAX_KEY, element.getType());
    }

    /**
     * Test method for {@link MaxKeyElement#toString()}.
     */
    @Test
    public void testToString() {
        final MaxKeyElement element = new MaxKeyElement("foo");

        assertEquals("\"foo\" : /* MAX_KEY */ 9223372036854775807",
                element.toString());
    }
}
