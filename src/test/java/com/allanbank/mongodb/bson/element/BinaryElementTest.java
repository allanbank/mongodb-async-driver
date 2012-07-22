/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static junit.framework.Assert.assertEquals;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
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
 * BinaryElementTest provides tests for the {@link BinaryElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BinaryElementTest {

    /**
     * Test method for
     * {@link BinaryElement#accept(com.allanbank.mongodb.bson.Visitor)}.
     */
    @Test
    public void testAccept() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitBinary(eq("foo"), eq((byte) 0x01),
                aryEq(element.getValue()));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for
     * {@link BinaryElement#BinaryElement(java.lang.String, byte, byte[])}.
     */
    @Test
    public void testBinaryElement() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals("foo", element.getName());
        assertEquals((byte) 0x01, element.getSubType());
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, element.getValue());
        assertEquals(ElementType.BINARY, element.getType());

        element = new BinaryElement("foo", (byte) 0x02, new byte[0]);
        assertEquals("foo", element.getName());
        assertEquals((byte) 0x02, element.getSubType());
        assertArrayEquals(new byte[0], element.getValue());
        assertEquals(ElementType.BINARY, element.getType());
    }

    /**
     * Test method for
     * {@link BinaryElement#BinaryElement(java.lang.String, byte[])}.
     */
    @Test
    public void testBinaryElementAltConstructor() {
        BinaryElement element = new BinaryElement("foo", new byte[] { 0x01,
                0x02, 0x03 });

        assertEquals("foo", element.getName());
        assertEquals(BinaryElement.DEFAULT_SUB_TYPE, element.getSubType());
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, element.getValue());
        assertEquals(ElementType.BINARY, element.getType());

        element = new BinaryElement("foo", new byte[0]);
        assertEquals("foo", element.getName());
        assertEquals(BinaryElement.DEFAULT_SUB_TYPE, element.getSubType());
        assertArrayEquals(new byte[0], element.getValue());
        assertEquals(ElementType.BINARY, element.getType());
    }

    /**
     * Test method for
     * {@link BinaryElement#BinaryElement(java.lang.String, byte, byte[])}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testBinaryElementThrows() {

        new BinaryElement("foo", (byte) 0x01, null);
    }

    /**
     * Test method for {@link BinaryElement#equals} and
     * {@link BinaryElement#hashCode()}.
     */
    @Test
    public void testEqualsObject() {
        final Random rand = new Random(System.currentTimeMillis());

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "2", "foo", "bar", "baz")) {
            final int count = rand.nextInt(50) + 10;
            for (int i = 0; i < count; ++i) {
                final byte[] bytes = new byte[rand.nextInt(17) + 5];
                rand.nextBytes(bytes);

                final byte subType = (byte) rand.nextInt(128);

                objs1.add(new BinaryElement(name, subType, bytes));
                objs2.add(new BinaryElement(name, subType, bytes));
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
     * Test method for {@link BinaryElement#getSubType()} and
     * {@link BinaryElement#getValue()}.
     */
    @Test
    public void testGetSubType() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals((byte) 0x01, element.getSubType());
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, element.getValue());
    }

    /**
     * Test method for {@link BinaryElement#queryPath}.
     */
    @Test
    public void testQueryPathWithoutTypeMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> elements = element.queryPath();
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));
    }

    /**
     * Test method for {@link BinaryElement#queryPath}.
     */
    @Test
    public void testQueryPathWithoutTypeNoMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> elements = element.queryPath("anything");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link BinaryElement#queryPath}.
     */
    @Test
    public void testQueryPathWithoutTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<BinaryElement> bElements = element
                .queryPath(BinaryElement.class);
        assertEquals(1, bElements.size());
        assertSame(element, bElements.get(0));

        final List<Element> elements = element.queryPath(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));
    }

    /**
     * Test method for {@link BinaryElement#queryPath}.
     */
    @Test
    public void testQueryPathWithTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> elements = element.queryPath(Element.class, "foo");
        assertEquals(0, elements.size());

        final List<BooleanElement> bElements = element
                .queryPath(BooleanElement.class);
        assertEquals(0, bElements.size());
    }

    /**
     * Test method for {@link BinaryElement#toString()}.
     */
    @Test
    public void testToString() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals("\"foo\" : (0x01) 0x010203", element.toString());

        element = new BinaryElement("foo", (byte) 0x11, new byte[] { 0x31,
                0x22, 0x13 });

        assertEquals("\"foo\" : (0x11) 0x312213", element.toString());
    }
}
