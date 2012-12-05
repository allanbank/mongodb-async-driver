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
 * SymbolElementTest provides tests for the {@link SymbolElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SymbolElementTest {

    /**
     * Test method for
     * {@link SymbolElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final SymbolElement element = new SymbolElement("foo", "string");

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitSymbol(eq("foo"), eq("string"));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link SymbolElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final String value = "" + random.nextLong();
                objs1.add(new SymbolElement(name, value));
                objs2.add(new SymbolElement(name, value));
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
     * Test method for {@link SymbolElement#getSymbol()}.
     */
    @Test
    public void testGetValue() {
        final SymbolElement element = new SymbolElement("foo", "string");

        assertEquals("string", element.getSymbol());
    }

    /**
     * Test method for
     * {@link SymbolElement#SymbolElement(java.lang.String, String)} .
     */
    @Test
    public void testSymbolElement() {
        final SymbolElement element = new SymbolElement("foo", "string");

        assertEquals("foo", element.getName());
        assertEquals("string", element.getSymbol());
        assertEquals(ElementType.SYMBOL, element.getType());
    }

    /**
     * Test method for {@link SymbolElement#SymbolElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new SymbolElement(null, "s");
    }

    /**
     * Test method for {@link SymbolElement#SymbolElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullValue() {

        new SymbolElement("s", null);
    }

    /**
     * Test method for {@link SymbolElement#toString()}.
     */
    @Test
    public void testToString() {
        final SymbolElement element = new SymbolElement("foo", "string");

        assertEquals("\"foo\" : string", element.toString());
    }

    /**
     * Test method for {@link SymbolElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        SymbolElement element = new SymbolElement("foo", "string");

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals("string", element.getSymbol());
        assertEquals(ElementType.SYMBOL, element.getType());
    }
}
