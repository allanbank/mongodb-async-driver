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

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * JavaScriptWithScopeElementTest provides tests for the
 * {@link JavaScriptWithScopeElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
     * Test method for
     * {@link JavaScriptWithScopeElement#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "foo", "bar", "baz", "2",
                null)) {
            for (final String code : Arrays.asList("1", "foo", "bar", "baz",
                    "2", null)) {
                objs1.add(new JavaScriptWithScopeElement(name, code, SCOPE_1));
                objs2.add(new JavaScriptWithScopeElement(name, code, SCOPE_1));

                objs1.add(new JavaScriptWithScopeElement(name, code, SCOPE_2));
                objs2.add(new JavaScriptWithScopeElement(name, code, SCOPE_2));

                objs1.add(new JavaScriptWithScopeElement(name, code, null));
                objs2.add(new JavaScriptWithScopeElement(name, code, null));
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
     * Test method for {@link JavaScriptWithScopeElement#toString()}.
     */
    @Test
    public void testToString() {
        final JavaScriptWithScopeElement element = new JavaScriptWithScopeElement(
                "foo", "func code() {}", SCOPE_1);

        assertEquals("\"foo\" : func code() {} (scope :{ \"f\" : true}\n)",
                element.toString());
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
}
