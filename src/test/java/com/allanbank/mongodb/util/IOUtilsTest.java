/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;

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
}
