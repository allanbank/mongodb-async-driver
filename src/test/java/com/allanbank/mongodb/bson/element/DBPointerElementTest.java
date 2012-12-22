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
 * DBPointerElementTest provides tests for the {@link DBPointerElement}
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("deprecation")
public class DBPointerElementTest {

    /**
     * Test method for
     * {@link DBPointerElement#accept(com.allanbank.mongodb.bson.Visitor)}.
     */
    @Test
    public void testAccept() {
        final ObjectId id = new ObjectId();
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitDBPointer(eq("foo"), eq("bar"), eq("baz"), eq(id));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for
     * {@link DBPointerElement#DBPointerElement(java.lang.String, java.lang.String, java.lang.String, com.allanbank.mongodb.bson.element.ObjectId)}
     * .
     */
    @Test
    public void testDBPointerElement() {
        final ObjectId id = new ObjectId();
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        assertEquals("foo", element.getName());
        assertEquals("bar", element.getDatabaseName());
        assertEquals("baz", element.getCollectionName());
        assertSame(id, element.getId());
        assertEquals(ElementType.DB_POINTER, element.getType());
    }

    /**
     * Test method for {@link DBPointerElement#DBPointerElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDBPointerElementThrowsOnNullCollection() {

        new DBPointerElement("foo", "d", null, new ObjectId());
    }

    /**
     * Test method for {@link DBPointerElement#DBPointerElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDBPointerElementThrowsOnNullDb() {

        new DBPointerElement("foo", null, "c", new ObjectId());
    }

    /**
     * Test method for {@link DBPointerElement#DBPointerElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDBPointerElementThrowsOnNullId() {

        new DBPointerElement("foo", "d", "c", null);
    }

    /**
     * Test method for {@link DBPointerElement#DBPointerElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testDBPointerElementThrowsOnNullName() {

        new DBPointerElement(null, "d", "c", new ObjectId());
    }

    /**
     * Test method for {@link DBPointerElement#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (final String dbName : Arrays.asList("1", "foo", "bar", "baz",
                    "2")) {
                for (final String cName : Arrays.asList("1", "foo", "bar",
                        "baz", "2")) {
                    for (int i = 0; i < 5; ++i) {

                        ObjectId id = new ObjectId();
                        objs1.add(new DBPointerElement(name, dbName, cName, id));
                        objs2.add(new DBPointerElement(name, dbName, cName, id));

                        id = new ObjectId();
                        objs1.add(new DBPointerElement(name, dbName, cName, id));
                        objs2.add(new DBPointerElement(name, dbName, cName, id));
                    }
                }
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
            assertFalse(obj1.equals(new BooleanElement(obj1.getName(), false)));
        }
    }

    /**
     * Test method for {@link DBPointerElement#toString()}.
     */
    @Test
    public void testToString() {
        final ObjectId id = new ObjectId(0x50d615d2, 0x8544eba9a10004e8L);
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        assertEquals(
                "foo : DBPointer( 'bar', 'baz', ObjectId('50d615d28544eba9a10004e8') )",
                element.toString());
    }

    /**
     * Test method for {@link DBPointerElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        final ObjectId id = new ObjectId();
        DBPointerElement element = new DBPointerElement("foo", "bar", "baz", id);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals("bar", element.getDatabaseName());
        assertEquals("baz", element.getCollectionName());
        assertSame(id, element.getId());
        assertEquals(ElementType.DB_POINTER, element.getType());
    }

}
