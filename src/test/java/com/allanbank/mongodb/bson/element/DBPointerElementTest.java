/*
 * #%L
 * DBPointerElementTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentReference;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * DBPointerElementTest provides tests for the {@link DBPointerElement}
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings({ "deprecation", "javadoc" })
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
     * Test method for {@link DBPointerElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final ObjectId oid1 = new ObjectId();
        final ObjectId oid2 = new ObjectId();

        final DBPointerElement a1 = new DBPointerElement("a", "a", "a", oid1);
        final DBPointerElement a11 = new DBPointerElement("a", "b", "a", oid1);
        final DBPointerElement a2 = new DBPointerElement("a", "a", "b", oid1);
        final DBPointerElement a3 = new DBPointerElement("a", "a", "a", oid2);
        final DBPointerElement b1 = new DBPointerElement("b", "a", "a", oid1);
        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));
        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a3) < 0);
        assertTrue(a3.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
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
     * Test method for {@link DBPointerElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final ObjectId id = new ObjectId(0x50d615d2, 0x8544eba9a10004e8L);
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        assertEquals(new DocumentReference("bar", "baz", new ObjectIdElement(
                "_id", id)), element.getValueAsObject());
    }

    /**
     * Test method for {@link DBPointerElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final ObjectId id = new ObjectId(0x50d615d2, 0x8544eba9a10004e8L);
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        assertEquals(
                "DBPointer( 'bar', 'baz', ObjectId('50d615d28544eba9a10004e8') )",
                element.getValueAsString());
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

    /**
     * Test method for {@link DBPointerElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final ObjectId id = new ObjectId();
        final DBPointerElement element = new DBPointerElement("foo", "bar",
                "baz", id);

        assertSame(element, element.withName("foo"));
    }
}
