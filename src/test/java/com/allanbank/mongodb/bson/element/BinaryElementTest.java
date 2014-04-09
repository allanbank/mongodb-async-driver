/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.io.BsonInputStream;

/**
 * BinaryElementTest provides tests for the {@link BinaryElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * {@link BinaryElement#BinaryElement(String, byte, BsonInputStream, int) )}
     * .
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBinaryElementStreamConstructor() throws IOException {
        final ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {
                0x01, 0x02, 0x03 });
        final BsonInputStream bin = new BsonInputStream(in);

        final BinaryElement element = new BinaryElement("foo",
                BinaryElement.DEFAULT_SUB_TYPE, bin, 3);

        assertEquals("foo", element.getName());
        assertEquals(BinaryElement.DEFAULT_SUB_TYPE, element.getSubType());
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, element.getValue());
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
     * Test method for
     * {@link BinaryElement#BinaryElement(java.lang.String, byte, byte[])}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testBinaryElementThrowsOnNullName() {

        new BinaryElement(null, (byte) 0x01, new byte[0]);
    }

    /**
     * Test method for {@link BinaryElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final BinaryElement a1 = new BinaryElement("a", (byte) 0, new byte[1]);
        final BinaryElement a12 = new BinaryElement("a", (byte) 0,
                new byte[] { 1 });
        final BinaryElement a11 = new BinaryElement("a", (byte) 0, new byte[2]);
        final BinaryElement a2 = new BinaryElement("a", (byte) 1, new byte[1]);
        final BinaryElement b1 = new BinaryElement("b", (byte) 0, new byte[1]);
        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));

        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a12) < 0);
        assertTrue(a12.compareTo(a1) > 0);

        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a1) > 0);

        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);

        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
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
     * Test method for {@link BinaryElement#findFirst}.
     */
    @Test
    public void testFindFirstWithoutTypeMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final Element found = element.findFirst();
        assertSame(element, found);
    }

    /**
     * Test method for {@link BinaryElement#findFirst}.
     */
    @Test
    public void testFindFirstWithoutTypeNoMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final Element found = element.findFirst("anything");
        assertNull(found);
    }

    /**
     * Test method for {@link BinaryElement#findFirst}.
     */
    @Test
    public void testFindFirstWithoutTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final BinaryElement bFound = element.findFirst(BinaryElement.class);
        assertSame(element, bFound);

        final Element found = element.findFirst(Element.class);
        assertSame(element, found);
    }

    /**
     * Test method for {@link BinaryElement#findFirst}.
     */
    @Test
    public void testFindFirstWithTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final Element found = element.findFirst(Element.class, "foo");
        assertNull(found);

        final BooleanElement bFound = element.findFirst(BooleanElement.class);
        assertNull(bFound);
    }

    /**
     * Test method for {@link BinaryElement#find}.
     */
    @Test
    public void testFindWithoutTypeMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> elements = element.find();
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));
    }

    /**
     * Test method for {@link BinaryElement#find}.
     */
    @Test
    public void testFindWithoutTypeNoMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> elements = element.find("anything");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link BinaryElement#find}.
     */
    @Test
    public void testFindWithoutTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<BinaryElement> bElements = element.find(BinaryElement.class);
        assertEquals(1, bElements.size());
        assertSame(element, bElements.get(0));

        final List<Element> elements = element.find(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));
    }

    /**
     * Test method for {@link BinaryElement#find}.
     */
    @Test
    public void testFindWithTypeNotMatch() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        final List<Element> found = element.find(Element.class, "foo");
        assertEquals(0, found.size());

        final List<BooleanElement> bFound = element.find(BooleanElement.class);
        assertEquals(0, bFound.size());
    }

    /**
     * Test method for {@link BinaryElement#get(int)}.
     */
    @Test
    public void testGet() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals(0x01, element.get(0));
        assertEquals(0x02, element.get(1));
        assertEquals(0x03, element.get(2));

        try {
            element.get(3);
            fail("Should have thrown an IndexOutOfBoundsException");
        }
        catch (final IndexOutOfBoundsException ioobe) {
            // Good.
        }
        try {
            element.get(-1);
            fail("Should have thrown an IndexOutOfBoundsException");
        }
        catch (final IndexOutOfBoundsException ioobe) {
            // Good.
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
     * Test method for {@link BinaryElement#length()}.
     */
    @Test
    public void testLength() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals(3, element.length());
    }

    /**
     * Test method for {@link BinaryElement#toString()}.
     */
    @Test
    public void testToString() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals("foo : BinData( 1, 'AQID' )", element.toString());

        element = new BinaryElement("foo", (byte) 0x11, new byte[] { 0x31,
                0x22, 0x13 });

        assertEquals("foo : BinData( 17, 'MSIT' )", element.toString());
    }

    /**
     * Test method for {@link BinaryElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertArrayEquals(element.getValue(),
                (byte[]) element.getValueAsObject());

        element = new BinaryElement("foo", (byte) 0x11, new byte[] { 0x31,
                0x22, 0x13 });

        assertArrayEquals(element.getValue(),
                (byte[]) element.getValueAsObject());
    }

    /**
     * Test method for {@link BinaryElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertEquals("BinData( 1, 'AQID' )", element.getValueAsString());

        element = new BinaryElement("foo", (byte) 0x11, new byte[] { 0x31,
                0x22, 0x13 });

        assertEquals("BinData( 17, 'MSIT' )", element.getValueAsString());
    }

    /**
     * Test method for {@link BinaryElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });
        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals((byte) 0x01, element.getSubType());
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, element.getValue());
        assertEquals(ElementType.BINARY, element.getType());
    }

    /**
     * Test method for {@link BinaryElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final BinaryElement element = new BinaryElement("foo", (byte) 0x01,
                new byte[] { 0x01, 0x02, 0x03 });

        assertSame(element, element.withName("foo"));
    }

}
