/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;

/**
 * UuidElementTest provides tests for the {@link UuidElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UuidElementTest {

    /** A test UUID with a very clear byte pattern. */
    public static final UUID TEST_UUID = new UUID(0x0011223344556677L,
            0x8899aabbccddeeffL);

    /** The legacy byte order for the TEST_UUID. */
    private final static byte[] LEGACY_BYTES = new byte[] { (byte) 0x77,
            (byte) 0x66, (byte) 0x55, (byte) 0x44, (byte) 0x33, (byte) 0x22,
            (byte) 0x11, (byte) 0x00, (byte) 0xff, (byte) 0xee, (byte) 0xdd,
            (byte) 0xcc, (byte) 0xbb, (byte) 0xaa, (byte) 0x99, (byte) 0x88 };

    /** The standard byte order for the TEST_UUID. */
    private final static byte[] STANDARD_BYTES = new byte[] { (byte) 0x00,
            (byte) 0x11, (byte) 0x22, (byte) 0x33, (byte) 0x44, (byte) 0x55,
            (byte) 0x66, (byte) 0x77, (byte) 0x88, (byte) 0x99, (byte) 0xaa,
            (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff };

    /**
     * Test method for {@link UuidElement#equals} and
     * {@link UuidElement#hashCode()}.
     */
    @Test
    public void testEqualsObject() {
        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "2", "foo", "bar", "baz")) {
            for (final byte subtype : new byte[] {
                    UuidElement.LEGACY_UUID_SUBTTYPE, UuidElement.UUID_SUBTTYPE }) {
                for (final UUID uuid : new UUID[] { UUID.randomUUID(),
                        UUID.randomUUID() }) {
                    objs1.add(new UuidElement(name, subtype, uuid));
                    objs2.add(new UuidElement(name, subtype, uuid));
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
     * Test method for {@link UuidElement#UuidElement(String, byte, byte[])}.
     */
    @Test
    public void testUuidElementStringByteByteArray() {

        UuidElement element = new UuidElement("f",
                UuidElement.LEGACY_UUID_SUBTTYPE, LEGACY_BYTES);
        assertEquals("f : BinData( 3, 'd2ZVRDMiEQD/7t3Mu6qZiA==' )",
                element.toString());
        assertEquals(UuidElement.LEGACY_UUID_SUBTTYPE, element.getSubType());
        assertArrayEquals(LEGACY_BYTES, element.getValue());
        assertEquals(TEST_UUID, element.getUuid());

        element = new UuidElement("f", UuidElement.UUID_SUBTTYPE,
                STANDARD_BYTES);
        assertEquals("f : BinData( 4, 'ABEiM0RVZneImaq7zN3u/w==' )",
                element.toString());
        assertEquals(UuidElement.UUID_SUBTTYPE, element.getSubType());
        assertArrayEquals(STANDARD_BYTES, element.getValue());
        assertEquals(TEST_UUID, element.getUuid());

    }

    /**
     * Test method for {@link UuidElement#UuidElement(String, byte, UUID)}.
     */
    @Test
    public void testUuidElementStringByteUUID() {
        UuidElement element = new UuidElement("f", UuidElement.UUID_SUBTTYPE,
                TEST_UUID);
        assertEquals("f : BinData( 4, 'ABEiM0RVZneImaq7zN3u/w==' )",
                element.toString());
        assertEquals(UuidElement.UUID_SUBTTYPE, element.getSubType());
        assertArrayEquals(STANDARD_BYTES, element.getValue());
        assertEquals(TEST_UUID, element.getUuid());

        element = new UuidElement("f", UuidElement.LEGACY_UUID_SUBTTYPE,
                TEST_UUID);
        assertEquals("f : BinData( 3, 'd2ZVRDMiEQD/7t3Mu6qZiA==' )",
                element.toString());
        assertEquals(UuidElement.LEGACY_UUID_SUBTTYPE, element.getSubType());
        assertArrayEquals(LEGACY_BYTES, element.getValue());
        assertEquals(TEST_UUID, element.getUuid());
    }

    /**
     * Test method for {@link UuidElement#UuidElement(String, UUID)}.
     */
    @Test
    public void testUuidElementStringUUID() {
        final UuidElement element = new UuidElement("f", TEST_UUID);
        assertEquals("f : BinData( 4, 'ABEiM0RVZneImaq7zN3u/w==' )",
                element.toString());
        assertEquals(UuidElement.UUID_SUBTTYPE, element.getSubType());
        assertArrayEquals(STANDARD_BYTES, element.getValue());
        assertEquals(TEST_UUID, element.getUuid());
    }

    /**
     * Test method for {@link UuidElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final UuidElement element = new UuidElement("f", TEST_UUID);

        assertEquals(TEST_UUID, element.getValueAsObject());
    }

    /**
     * Test method for {@link UuidElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final UuidElement element = new UuidElement("f", TEST_UUID);

        assertEquals(TEST_UUID.toString(), element.getValueAsString());
    }

    /**
     * Test method for {@link UuidElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        final UuidElement element = new UuidElement("foo",
                UuidElement.LEGACY_UUID_SUBTTYPE, UUID.randomUUID());
        final UuidElement newElement = element.withName("bar");
        assertEquals("bar", newElement.getName());
        assertEquals(UuidElement.LEGACY_UUID_SUBTTYPE, newElement.getSubType());
        assertEquals(element.getUuid(), newElement.getUuid());
        assertArrayEquals(element.getValue(), newElement.getValue());
        assertEquals(ElementType.BINARY, element.getType());
    }
}
