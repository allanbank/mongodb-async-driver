/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

/**
 * IOUtilsTest provides tests for the {@link IOUtils} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IOUtilsTest {
    /**
     * Test method for {@link IOUtils#toHex(byte[])}.
     */
    @Test
    public void testAsHex() {
        assertEquals("00", IOUtils.toHex(new byte[] { (byte) 0x00 }));
        assertEquals("01", IOUtils.toHex(new byte[] { (byte) 0x01 }));
        assertEquals("02", IOUtils.toHex(new byte[] { (byte) 0x02 }));
        assertEquals("03", IOUtils.toHex(new byte[] { (byte) 0x03 }));
        assertEquals("04", IOUtils.toHex(new byte[] { (byte) 0x04 }));
        assertEquals("05", IOUtils.toHex(new byte[] { (byte) 0x05 }));
        assertEquals("06", IOUtils.toHex(new byte[] { (byte) 0x06 }));
        assertEquals("07", IOUtils.toHex(new byte[] { (byte) 0x07 }));
        assertEquals("08", IOUtils.toHex(new byte[] { (byte) 0x08 }));
        assertEquals("09", IOUtils.toHex(new byte[] { (byte) 0x09 }));
        assertEquals("0a", IOUtils.toHex(new byte[] { (byte) 0x0a }));
        assertEquals("0b", IOUtils.toHex(new byte[] { (byte) 0x0b }));
        assertEquals("0c", IOUtils.toHex(new byte[] { (byte) 0x0c }));
        assertEquals("0d", IOUtils.toHex(new byte[] { (byte) 0x0d }));
        assertEquals("0e", IOUtils.toHex(new byte[] { (byte) 0x0e }));
        assertEquals("0f", IOUtils.toHex(new byte[] { (byte) 0x0f }));
        assertEquals("1f", IOUtils.toHex(new byte[] { (byte) 0x1f }));
        assertEquals("2f", IOUtils.toHex(new byte[] { (byte) 0x2f }));
        assertEquals("3f", IOUtils.toHex(new byte[] { (byte) 0x3f }));
        assertEquals("4f", IOUtils.toHex(new byte[] { (byte) 0x4f }));
        assertEquals("5f", IOUtils.toHex(new byte[] { (byte) 0x5f }));
        assertEquals("6f", IOUtils.toHex(new byte[] { (byte) 0x6f }));
        assertEquals("7f", IOUtils.toHex(new byte[] { (byte) 0x7f }));
        assertEquals("8f", IOUtils.toHex(new byte[] { (byte) 0x8f }));
        assertEquals("9f", IOUtils.toHex(new byte[] { (byte) 0x9f }));
        assertEquals("af", IOUtils.toHex(new byte[] { (byte) 0xaf }));
        assertEquals("bf", IOUtils.toHex(new byte[] { (byte) 0xbf }));
        assertEquals("cf", IOUtils.toHex(new byte[] { (byte) 0xcf }));
        assertEquals("df", IOUtils.toHex(new byte[] { (byte) 0xdf }));
        assertEquals("ef", IOUtils.toHex(new byte[] { (byte) 0xef }));
        assertEquals("ff", IOUtils.toHex(new byte[] { (byte) 0xff }));
    }

    /**
     * Test method for {@link IOUtils#base64ToBytes(String)}.
     * 
     * @throws UnsupportedEncodingException
     *             On a failure to encode the test vector.
     */
    @Test
    public void testBase64ToBytes() throws UnsupportedEncodingException {
        assertArrayEquals(
                "The quick brown fox jumped over the lazy dogs."
                        .getBytes("US-ASCII"),
                IOUtils.base64ToBytes("VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg=="));
        assertArrayEquals(
                "It was the best of times, it was the worst of times."
                        .getBytes("US-ASCII"),
                IOUtils.base64ToBytes("SXQgd2FzIHRoZSBiZXN0IG9mIHRpbWVzLCBpdCB3YXMgdGhlIHdvcnN0IG9mIHRpbWVzLg=="));
        assertArrayEquals(
                "http://jakarta.apache.org/commmons".getBytes("US-ASCII"),
                IOUtils.base64ToBytes("aHR0cDovL2pha2FydGEuYXBhY2hlLm9yZy9jb21tbW9ucw=="));
        assertArrayEquals(
                "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz"
                        .getBytes("US-ASCII"),
                IOUtils.base64ToBytes("QWFCYkNjRGRFZUZmR2dIaElpSmpLa0xsTW1Obk9vUHBRcVJyU3NUdFV1VnZXd1h4WXlaeg=="));
        assertArrayEquals(
                "{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }".getBytes("US-ASCII"),
                IOUtils.base64ToBytes("eyAwLCAxLCAyLCAzLCA0LCA1LCA2LCA3LCA4LCA5IH0="));
        assertArrayEquals("xyzzy!".getBytes("US-ASCII"),
                IOUtils.base64ToBytes("eHl6enkh"));
    }

    /**
     * Test method for {@link IOUtils#close}.
     * 
     * @throws IOException
     *             On a failure setting up the test mocks.
     */
    @Test
    public void testClose() throws IOException {
        final Closeable mockCloseable = createMock(Closeable.class);

        mockCloseable.close();
        expectLastCall().andThrow(new IOException("This is a test."));

        replay(mockCloseable);

        IOUtils.close(mockCloseable);

        verify(mockCloseable);
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test
    public void testHexToBytes() {
        assertArrayEquals(IOUtils.hexToBytes(" "), new byte[0]);

        assertArrayEquals(IOUtils.hexToBytes("00"), new byte[] { (byte) 0x00 });
        assertArrayEquals(IOUtils.hexToBytes("01"), new byte[] { (byte) 0x01 });
        assertArrayEquals(IOUtils.hexToBytes("02"), new byte[] { (byte) 0x02 });
        assertArrayEquals(IOUtils.hexToBytes("03"), new byte[] { (byte) 0x03 });
        assertArrayEquals(IOUtils.hexToBytes("04"), new byte[] { (byte) 0x04 });
        assertArrayEquals(IOUtils.hexToBytes("05"), new byte[] { (byte) 0x05 });
        assertArrayEquals(IOUtils.hexToBytes("06"), new byte[] { (byte) 0x06 });
        assertArrayEquals(IOUtils.hexToBytes("07"), new byte[] { (byte) 0x07 });
        assertArrayEquals(IOUtils.hexToBytes("08"), new byte[] { (byte) 0x08 });
        assertArrayEquals(IOUtils.hexToBytes("09"), new byte[] { (byte) 0x09 });
        assertArrayEquals(IOUtils.hexToBytes("0a"), new byte[] { (byte) 0x0a });
        assertArrayEquals(IOUtils.hexToBytes("0b"), new byte[] { (byte) 0x0b });
        assertArrayEquals(IOUtils.hexToBytes("0c"), new byte[] { (byte) 0x0c });
        assertArrayEquals(IOUtils.hexToBytes("0d"), new byte[] { (byte) 0x0d });
        assertArrayEquals(IOUtils.hexToBytes("0e"), new byte[] { (byte) 0x0e });
        assertArrayEquals(IOUtils.hexToBytes("0f"), new byte[] { (byte) 0x0f });
        assertArrayEquals(IOUtils.hexToBytes("1f"), new byte[] { (byte) 0x1f });
        assertArrayEquals(IOUtils.hexToBytes("2f"), new byte[] { (byte) 0x2f });
        assertArrayEquals(IOUtils.hexToBytes("3f"), new byte[] { (byte) 0x3f });
        assertArrayEquals(IOUtils.hexToBytes("4f"), new byte[] { (byte) 0x4f });
        assertArrayEquals(IOUtils.hexToBytes("5f"), new byte[] { (byte) 0x5f });
        assertArrayEquals(IOUtils.hexToBytes("6f"), new byte[] { (byte) 0x6f });
        assertArrayEquals(IOUtils.hexToBytes("7f"), new byte[] { (byte) 0x7f });
        assertArrayEquals(IOUtils.hexToBytes("8f"), new byte[] { (byte) 0x8f });
        assertArrayEquals(IOUtils.hexToBytes("9f"), new byte[] { (byte) 0x9f });
        assertArrayEquals(IOUtils.hexToBytes("af"), new byte[] { (byte) 0xaf });
        assertArrayEquals(IOUtils.hexToBytes("bf"), new byte[] { (byte) 0xbf });
        assertArrayEquals(IOUtils.hexToBytes("cf"), new byte[] { (byte) 0xcf });
        assertArrayEquals(IOUtils.hexToBytes("df"), new byte[] { (byte) 0xdf });
        assertArrayEquals(IOUtils.hexToBytes("ef"), new byte[] { (byte) 0xef });
        assertArrayEquals(IOUtils.hexToBytes("ff"), new byte[] { (byte) 0xff });

        assertArrayEquals(IOUtils.hexToBytes("0A"), new byte[] { (byte) 0x0a });
        assertArrayEquals(IOUtils.hexToBytes("0B"), new byte[] { (byte) 0x0b });
        assertArrayEquals(IOUtils.hexToBytes("0C"), new byte[] { (byte) 0x0c });
        assertArrayEquals(IOUtils.hexToBytes("0D"), new byte[] { (byte) 0x0d });
        assertArrayEquals(IOUtils.hexToBytes("0E"), new byte[] { (byte) 0x0e });
        assertArrayEquals(IOUtils.hexToBytes("0F"), new byte[] { (byte) 0x0f });
        assertArrayEquals(IOUtils.hexToBytes("1F"), new byte[] { (byte) 0x1f });
        assertArrayEquals(IOUtils.hexToBytes("2F"), new byte[] { (byte) 0x2f });
        assertArrayEquals(IOUtils.hexToBytes("3F"), new byte[] { (byte) 0x3f });
        assertArrayEquals(IOUtils.hexToBytes("4F"), new byte[] { (byte) 0x4f });
        assertArrayEquals(IOUtils.hexToBytes("5F"), new byte[] { (byte) 0x5f });
        assertArrayEquals(IOUtils.hexToBytes("6F"), new byte[] { (byte) 0x6f });
        assertArrayEquals(IOUtils.hexToBytes("7F"), new byte[] { (byte) 0x7f });
        assertArrayEquals(IOUtils.hexToBytes("8F"), new byte[] { (byte) 0x8f });
        assertArrayEquals(IOUtils.hexToBytes("9F"), new byte[] { (byte) 0x9f });
        assertArrayEquals(IOUtils.hexToBytes("AF"), new byte[] { (byte) 0xaf });
        assertArrayEquals(IOUtils.hexToBytes("BF"), new byte[] { (byte) 0xbf });
        assertArrayEquals(IOUtils.hexToBytes("CF"), new byte[] { (byte) 0xcf });
        assertArrayEquals(IOUtils.hexToBytes("DF"), new byte[] { (byte) 0xdf });
        assertArrayEquals(IOUtils.hexToBytes("EF"), new byte[] { (byte) 0xef });
        assertArrayEquals(IOUtils.hexToBytes("FF"), new byte[] { (byte) 0xff });

        assertArrayEquals(
                new byte[] { 0x00, (byte) 0x01, (byte) 0x02, (byte) 0x03,
                        (byte) 0x04, (byte) 0x05, (byte) 0x06, (byte) 0x07,
                        (byte) 0x08, (byte) 0x09, (byte) 0x0a, (byte) 0x0b,
                        (byte) 0x0c, (byte) 0x0d, (byte) 0x0e, (byte) 0x0f,
                        (byte) 0x1f, (byte) 0x2f, (byte) 0x3f, (byte) 0x4f,
                        (byte) 0x5f, (byte) 0x6f, (byte) 0x7f, (byte) 0x8f,
                        (byte) 0x9f, (byte) 0xaf, (byte) 0xbf, (byte) 0xcf,
                        (byte) 0xdf, (byte) 0xef, (byte) 0xff },
                IOUtils.hexToBytes("000102030405060708090a0b0c0d0e0f1f2f3f4f5f6f7f8f9fafbfcfdfefff"));
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testHexToBytesThrowsOnAInvalidCharacter() {
        IOUtils.hexToBytes("012Z");
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testHexToBytesThrowsOnAInvalidCharacterAfter() {
        IOUtils.hexToBytes("012z");
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testHexToBytesThrowsOnAInvalidCharacterAfterFirstNibble() {
        IOUtils.hexToBytes("01z2");
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testHexToBytesThrowsOnAInvalidCharacterBefore() {
        IOUtils.hexToBytes("01!c");
    }

    /**
     * Test method for {@link IOUtils#hexToBytes}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testHexToBytesThrowsOnAInvalidStringLength() {
        IOUtils.hexToBytes("012");
    }

    /**
     * Test method for {@link IOUtils#toBase64(byte[])}.
     * 
     * @throws UnsupportedEncodingException
     *             On a failure to encode the test vector.
     */
    @Test
    public void testToBase64() throws UnsupportedEncodingException {
        assertEquals(
                "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==",
                IOUtils.toBase64("The quick brown fox jumped over the lazy dogs."
                        .getBytes("US-ASCII")));
        assertEquals(
                "YmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWggYmxhaCBibGFoIGJsYWg=",
                IOUtils.toBase64("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah"
                        .getBytes("US-ASCII")));
        assertEquals(
                "SXQgd2FzIHRoZSBiZXN0IG9mIHRpbWVzLCBpdCB3YXMgdGhlIHdvcnN0IG9mIHRpbWVzLg==",
                IOUtils.toBase64("It was the best of times, it was the worst of times."
                        .getBytes("US-ASCII")));
        assertEquals("aHR0cDovL2pha2FydGEuYXBhY2hlLm9yZy9jb21tbW9ucw==",
                IOUtils.toBase64("http://jakarta.apache.org/commmons"
                        .getBytes("US-ASCII")));
        assertEquals(
                "QWFCYkNjRGRFZUZmR2dIaElpSmpLa0xsTW1Obk9vUHBRcVJyU3NUdFV1VnZXd1h4WXlaeg==",
                IOUtils.toBase64("AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz"
                        .getBytes("US-ASCII")));
        assertEquals("eyAwLCAxLCAyLCAzLCA0LCA1LCA2LCA3LCA4LCA5IH0=",
                IOUtils.toBase64("{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }"
                        .getBytes("US-ASCII")));
        assertEquals("eHl6enkh",
                IOUtils.toBase64("xyzzy!".getBytes("US-ASCII")));
    }
}
