/*
 * #%L
 * JavaScriptElementTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * JavaScriptElementTest provides tests for the {@link JavaScriptElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptElementTest {

    /**
     * Test method for
     * {@link JavaScriptElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitJavaScript(eq("foo"), eq("func code() {}"));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link JavaScriptElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final JavaScriptElement a1 = new JavaScriptElement("a", "1");
        final JavaScriptElement a11 = new JavaScriptElement("a", "11");
        final JavaScriptElement b1 = new JavaScriptElement("b", "1");

        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for {@link JavaScriptElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (final String code : Arrays.asList("1", "foo", "bar", "baz",
                    "2")) {
                objs1.add(new JavaScriptElement(name, code));
                objs2.add(new JavaScriptElement(name, code));
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
     * Test method for {@link JavaScriptElement#getJavaScript()}.
     */
    @Test
    public void testGetJavaScript() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertEquals("func code() {}", element.getJavaScript());
    }

    /**
     * Test method for
     * {@link JavaScriptElement#JavaScriptElement(java.lang.String, String)} .
     */
    @Test
    public void testJavaScriptElement() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertEquals("foo", element.getName());
        assertEquals("func code() {}", element.getJavaScript());
        assertEquals(ElementType.JAVA_SCRIPT, element.getType());
    }

    /**
     * Test method for {@link JavaScriptElement#JavaScriptElement} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new JavaScriptElement(null, "func code() {}");
    }

    /**
     * Test method for {@link JavaScriptElement#JavaScriptElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullValue() {

        new JavaScriptElement("s", null);
    }

    /**
     * Test method for {@link JavaScriptElement#toString()}.
     */
    @Test
    public void testToString() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertEquals("foo : { $code : 'func code() {}' }", element.toString());
    }

    /**
     * Test method for {@link JavaScriptElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertEquals(element.getJavaScript(), element.getValueAsObject());
    }

    /**
     * Test method for {@link JavaScriptElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertEquals("{ $code : 'func code() {}' }", element.getValueAsString());
    }

    /**
     * Test method for {@link JavaScriptElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals("func code() {}", element.getJavaScript());
        assertEquals(ElementType.JAVA_SCRIPT, element.getType());
    }

    /**
     * Test method for {@link JavaScriptElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final JavaScriptElement element = new JavaScriptElement("foo",
                "func code() {}");

        assertSame(element, element.withName("foo"));
    }
}
