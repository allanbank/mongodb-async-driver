/*
 * #%L
 * MongoTimestampElementTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * MongoTimestampElementTest provides tests for the
 * {@link MongoTimestampElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoTimestampElementTest {

    /**
     * Test method for
     * {@link MongoTimestampElement#accept(com.allanbank.mongodb.bson.Visitor)}
     * .
     */
    @Test
    public void testAccept() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitMongoTimestamp(eq("foo"), eq(1010101L));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link MongoTimestampElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final MongoTimestampElement a1 = new MongoTimestampElement("a", 1);
        final MongoTimestampElement a11 = new MongoTimestampElement("a", 11);
        final MongoTimestampElement b1 = new MongoTimestampElement("b", 1);

        final TimestampElement i = new TimestampElement("a", 2);

        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(i) < 0);
        assertTrue(i.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for {@link MongoTimestampElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (int i = 0; i < 10; ++i) {
                final long value = random.nextLong();
                objs1.add(new MongoTimestampElement(name, value));
                objs2.add(new MongoTimestampElement(name, value));
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
     * Test method for {@link MongoTimestampElement#getTime()}.
     */
    @Test
    public void testGetValue() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertEquals(1010101L, element.getTime());
    }

    /**
     * Test method for
     * {@link MongoTimestampElement#MongoTimestampElement(java.lang.String, long)}
     * .
     */
    @Test
    public void testMongoTimestampElement() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertEquals("foo", element.getName());
        assertEquals(1010101, element.getTime(), 0.0001);
        assertEquals(ElementType.MONGO_TIMESTAMP, element.getType());
    }

    /**
     * Test method for {@link MongoTimestampElement#MongoTimestampElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new MongoTimestampElement(null, 1);
    }

    /**
     * Test method for {@link MongoTimestampElement#toString()}.
     */
    @Test
    public void testToString() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertEquals("foo : Timestamp(0, 1010101)", element.toString());
    }

    /**
     * Test method for {@link MongoTimestampElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertEquals(new Long(1010101), element.getValueAsObject());
    }

    /**
     * Test method for {@link MongoTimestampElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertEquals("Timestamp(0, 1010101)", element.getValueAsString());
    }

    /**
     * Test method for {@link MongoTimestampElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(1010101, element.getTime(), 0.0001);
        assertEquals(ElementType.MONGO_TIMESTAMP, element.getType());
    }

    /**
     * Test method for {@link MongoTimestampElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final MongoTimestampElement element = new MongoTimestampElement("foo",
                1010101);

        assertSame(element, element.withName("foo"));
    }
}
