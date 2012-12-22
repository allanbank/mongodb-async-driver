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
 * JavaScriptElementTest provides tests for the {@link JavaScriptElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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

        assertEquals("foo : func code() {}", element.toString());
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
}
