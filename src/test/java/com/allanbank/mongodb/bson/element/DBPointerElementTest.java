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
import static org.junit.Assert.*;

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
     * Test method for {@link DBPointerElement#equals(java.lang.Object)}.
     */
    @Test
    public void testEqualsObject() {
        List<Element> objs1 = new ArrayList<Element>();
        List<Element> objs2 = new ArrayList<Element>();

        for (String name : Arrays.asList("1", "foo", "bar", "baz", "2", null)) {
            for (String dbName : Arrays.asList("1", "foo", "bar", "baz", "2")) {
                for (String cName : Arrays
                        .asList("1", "foo", "bar", "baz", "2")) {
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
            Element obj1 = objs1.get(i);
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
        ObjectId id = new ObjectId();
        DBPointerElement element = new DBPointerElement("foo", "bar", "baz", id);

        assertEquals("\"foo\" : DBPointer( \"bar.baz\", " + id + ")",
                element.toString());
    }

    /**
     * Test method for
     * {@link DBPointerElement#DBPointerElement(java.lang.String, java.lang.String, java.lang.String, com.allanbank.mongodb.bson.element.ObjectId)}
     * .
     */
    @Test
    public void testDBPointerElement() {
        ObjectId id = new ObjectId();
        DBPointerElement element = new DBPointerElement("foo", "bar", "baz", id);

        assertEquals("foo", element.getName());
        assertEquals("bar", element.getDatabaseName());
        assertEquals("baz", element.getCollectionName());
        assertSame(id, element.getId());
        assertEquals(ElementType.DB_POINTER, element.getType());
    }

    /**
     * Test method for
     * {@link DBPointerElement#accept(com.allanbank.mongodb.bson.Visitor)}.
     */
    @Test
    public void testAccept() {
        ObjectId id = new ObjectId();
        DBPointerElement element = new DBPointerElement("foo", "bar", "baz", id);

        Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitDBPointer(eq("foo"), eq("bar"), eq("baz"), eq(id));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

}
