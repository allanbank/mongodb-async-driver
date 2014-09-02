/*
 * #%L
 * JavaScriptWithScopeElementTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * JavaScriptWithScopeElementTest provides tests for the
 * {@link JavaScriptWithScopeElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JavaScriptWithScopeElementTest {

    /** A sample scope document. */
    public static final Document SCOPE_1 = BuilderFactory.start()
            .addBoolean("f", true).build();

    /** A sample scope document. */
    public static final Document SCOPE_2 = BuilderFactory.start()
            .addBoolean("f", false).build();

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#accept(com.allanbank.mongodb.bson.Visitor)}
     * .
     */
    @Test
    public void testAccept() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitJavaScript(eq("foo"), eq("func code() {}"),
                eq(SCOPE_1));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final Document d1 = BuilderFactory.start().build();
        final Document d2 = BuilderFactory.start().add("a", 1).build();
        final Document d3 = BuilderFactory.start().add("a", 2).build();
        final Document d4 = BuilderFactory.start().add("a", 2).add("b", 3)
                .build();
        final Document d5 = BuilderFactory.start().add("a", 2).add("b", 4)
                .build();

        final JavaScriptWithScopeElement a1 = new JavaScriptWithScopeElement(
                "a", "1", d1);
        final JavaScriptWithScopeElement a11 = new JavaScriptWithScopeElement(
                "a", "11", d1);
        final JavaScriptWithScopeElement a12 = new JavaScriptWithScopeElement(
                "a", "1", d2);
        final JavaScriptWithScopeElement a13 = new JavaScriptWithScopeElement(
                "a", "1", d3);
        final JavaScriptWithScopeElement a14 = new JavaScriptWithScopeElement(
                "a", "1", d4);
        final JavaScriptWithScopeElement a15 = new JavaScriptWithScopeElement(
                "a", "1", d5);
        final JavaScriptWithScopeElement b1 = new JavaScriptWithScopeElement(
                "b", "1", d1);

        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a12) < 0);
        assertTrue(a12.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a13) < 0);
        assertTrue(a13.compareTo(a1) > 0);

        assertEquals(0, a13.compareTo(a13));
        assertTrue(a13.compareTo(a14) < 0);
        assertTrue(a14.compareTo(a13) > 0);

        assertEquals(0, a14.compareTo(a14));
        assertTrue(a14.compareTo(a15) < 0);
        assertTrue(a15.compareTo(a14) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2")) {
            for (final String code : Arrays.asList("1", "foo", "bar", "baz",
                    "2")) {
                objs1.add(new JavaScriptWithScopeElement(name, code, SCOPE_1));
                objs2.add(new JavaScriptWithScopeElement(name, code, SCOPE_1));

                objs1.add(new JavaScriptWithScopeElement(name, code, SCOPE_2));
                objs2.add(new JavaScriptWithScopeElement(name, code, SCOPE_2));
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
     * Test method for {@link JavaScriptWithScopeElement#getJavaScript()}.
     */
    @Test
    public void testGetJavaScript() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals("func code() {}", element.getJavaScript());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#getScope()}.
     */
    @Test
    public void testGetScope() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals(SCOPE_1, element.getScope());
    }

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#JavaScriptWithScopeElement(String, String, Document)}
     * .
     */
    @Test
    public void testJavaScriptWithScopeElement() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals("foo", element.getName());
        assertEquals("func code() {}", element.getJavaScript());
        assertEquals(SCOPE_1, element.getScope());
        assertEquals(ElementType.JAVA_SCRIPT_WITH_SCOPE, element.getType());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#size()}.
     */
    @Test
    public void testSize() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertThat(element.size(), is(37L));
    }

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#JavaScriptWithScopeElement} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new JavaScriptWithScopeElement(null, "func code() {}", SCOPE_1);
    }

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#JavaScriptWithScopeElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullScope() {

        new JavaScriptWithScopeElement("s", "func code() {}", null);
    }

    /**
     * Test method for
     * {@link JavaScriptWithScopeElement#JavaScriptWithScopeElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullValue() {

        new JavaScriptWithScopeElement("s", null, SCOPE_1);
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#toString()}.
     */
    @Test
    public void testToString() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals(
                "foo : { $code : 'func code() {}', $scope : { f : true } }",
                element.toString());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        final DocumentBuilder b = BuilderFactory.start();
        b.add("$code", element.getJavaScript());
        b.add("$scope", element.getScope());

        assertEquals(b.build(), element.getValueAsObject());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals("{ $code : 'func code() {}', $scope : { f : true } }",
                element.getValueAsString());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals("func code() {}", element.getJavaScript());
        assertEquals(SCOPE_1, element.getScope());
        assertEquals(ElementType.JAVA_SCRIPT_WITH_SCOPE, element.getType());
    }

    /**
     * Test method for {@link JavaScriptWithScopeElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertSame(element, element.withName("foo"));
    }
}
