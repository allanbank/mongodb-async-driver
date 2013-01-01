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
 * BooleanElementTest provides tests for the {@link BooleanElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BooleanElementTest {

    /**
     * Test method for
     * {@link BooleanElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final BooleanElement element = new BooleanElement("foo", false);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitBoolean(eq("foo"), eq(false));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for
     * {@link BooleanElement#BooleanElement(java.lang.String, boolean)} .
     */
    @Test
    public void testBooleanElement() {
        final BooleanElement element = new BooleanElement("foo", false);

        assertEquals("foo", element.getName());
        assertEquals(false, element.getValue());
        assertEquals(ElementType.BOOLEAN, element.getType());
    }

    /**
     * Test method for {@link BooleanElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            objs1.add(new BooleanElement(name, false));
            objs2.add(new BooleanElement(name, false));
            objs1.add(new BooleanElement(name, true));
            objs2.add(new BooleanElement(name, true));
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
     * Test method for {@link BooleanElement#BooleanElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new BooleanElement(null, true);
    }

    /**
     * Test method for {@link BooleanElement#toString()}.
     */
    @Test
    public void testToString() {
        final BooleanElement element = new BooleanElement("foo", false);

        assertEquals("foo : false", element.toString());
    }

    /**
     * Test method for {@link BooleanElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        BooleanElement element = new BooleanElement("foo", false);
        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(false, element.getValue());
        assertEquals(ElementType.BOOLEAN, element.getType());
    }

}
