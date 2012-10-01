/*
 * Copyright 2012, Allanbank Consulting, Inc. 
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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * ObjectIdElementTest provides tests for the {@link ObjectIdElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ObjectIdElementTest {

    /**
     * Test method for
     * {@link ObjectIdElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final ObjectIdElement element = new ObjectIdElement("foo",
                new ObjectId());

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitObjectId(eq("foo"), eq(element.getId()));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link ObjectIdElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final ObjectId oid1 = new ObjectId();
        final ObjectId oid2 = new ObjectId();

        final ObjectIdElement a1 = new ObjectIdElement("a", oid1);
        final ObjectIdElement a2 = new ObjectIdElement("a", oid2);
        final ObjectIdElement b1 = new ObjectIdElement("b", oid1);
        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for
     * {@link ObjectIdElement#ObjectIdElement(java.lang.String, ObjectId)} .
     */
    @Test(expected = AssertionError.class)
    @SuppressWarnings("unused")
    public void testConstructorWithNullId() {
        new ObjectIdElement("foo", null);
    }

    /**
     * Test method for {@link ObjectIdElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final ObjectId id = new ObjectId();

                objs1.add(new ObjectIdElement(name, id));
                objs2.add(new ObjectIdElement(name, id));
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
                assertFalse("" + obj1 + " != " + obj2,
                        obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new MaxKeyElement(obj1.getName())));
        }
    }

    /**
     * Test method for {@link ObjectIdElement#getId()}.
     */
    @Test
    public void testGetValue() {
        final ObjectId id = new ObjectId();
        final ObjectIdElement element = new ObjectIdElement("foo", id);

        assertEquals(id, element.getId());
    }

    /**
     * Test method for
     * {@link ObjectIdElement#ObjectIdElement(java.lang.String, ObjectId)} .
     */
    @Test
    public void testObjectIdElement() {
        final ObjectId id = new ObjectId();
        final ObjectIdElement element = new ObjectIdElement("foo", id);

        assertEquals("foo", element.getName());
        assertEquals(id, element.getId());
        assertEquals(ElementType.OBJECT_ID, element.getType());
    }

    /**
     * Test method for {@link ObjectIdElement#ObjectIdElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new ObjectIdElement(null, new ObjectId());
    }

    /**
     * Test method for {@link ObjectIdElement#ObjectIdElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullValue() {

        new ObjectIdElement("s", null);
    }

    /**
     * Test method for {@link ObjectIdElement#toString()}.
     */
    @Test
    public void testToString() {
        final ObjectIdElement element = new ObjectIdElement("foo",
                new ObjectId(0x11223344, 0x1122334455667788L));

        assertEquals("foo : ObjectId('112233441122334455667788')",
                element.toString());
    }

    /**
     * Test method for {@link ObjectIdElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        final ObjectId id = new ObjectId();
        ObjectIdElement element = new ObjectIdElement("foo", id);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(id, element.getId());
        assertEquals(ElementType.OBJECT_ID, element.getType());
    }
}
