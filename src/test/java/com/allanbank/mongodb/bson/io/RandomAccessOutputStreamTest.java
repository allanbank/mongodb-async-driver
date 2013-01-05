/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link RandomAccessOutputStream} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RandomAccessOutputStreamTest {

    /** The stream being tested. */
    private RandomAccessOutputStream myTestStream = null;

    /**
     * Sets up the test stream.
     */
    @Before
    public void setUp() {
        myTestStream = new RandomAccessOutputStream();
    }

    /**
     * Closes the test stream.
     */
    @After
    public void tearDown() {
        myTestStream.close();
        myTestStream = null;
    }

    /**
     * Test method for {@link RandomAccessOutputStream#flush()}.
     */
    @Test
    public void testFlush() {
        myTestStream.flush();
    }

    /**
     * Test method for {@link RandomAccessOutputStream#reset()}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testReset() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        final Random rand = new Random(System.currentTimeMillis());

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
        }
        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
        }

        final int offset = rand.nextInt(2048);
        final int length = rand.nextInt(2048);
        myTestStream.write(bOut.toByteArray(), offset, length);

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);
        assertArrayEquals(
                "Byte arrays are not the same.",
                Arrays.copyOfRange(bOut.toByteArray(), offset, offset + length),
                finalOut.toByteArray());

        // Reset and write the contents again.
        myTestStream.reset();
        finalOut.reset();
        myTestStream.writeTo(finalOut);
        assertArrayEquals("Byte arrays are not the same after reset.",
                new byte[0], finalOut.toByteArray());
    }

    /**
     * Test method for {@link RandomAccessOutputStream#writeAt(long, byte[])}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteAtLongByteArray() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final Random rand = new Random(System.currentTimeMillis());

        // First fill the test stream with random data.
        for (int i = 0; i < (4096 + 256); ++i) {
            myTestStream.write(0);
        }

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
        }

        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
        }

        // Now copy over a segment at a time.
        final byte[] fullBuffer = bOut.toByteArray();
        int toCopy = fullBuffer.length;
        while (toCopy > 0) {
            final int copy = rand.nextInt(toCopy) + 1;
            final int start = fullBuffer.length - toCopy;

            myTestStream.writeAt(start,
                    Arrays.copyOfRange(fullBuffer, start, start + copy));

            toCopy -= copy;
        }

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }

    /**
     * Test method for
     * {@link RandomAccessOutputStream#writeAt(long, byte[], int, int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteAtLongByteArrayIntInt() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final Random rand = new Random(System.currentTimeMillis());

        // First fill the test stream with random data.
        for (int i = 0; i < (16700 + 256); ++i) {
            myTestStream.write(0);
        }

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
        }

        // Now some random value.
        for (int i = 0; i < 16700; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
        }

        // Now copy over a segment at a time.
        final byte[] fullBuffer = bOut.toByteArray();
        int toCopy = fullBuffer.length;
        while (toCopy > 0) {
            final int copy = rand.nextInt(toCopy) + 1;
            final int start = fullBuffer.length - toCopy;

            myTestStream.writeAt(start, fullBuffer, start, copy);

            toCopy -= copy;
        }

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }

    /**
     * Test method for
     * {@link RandomAccessOutputStream#writeAt(long, byte[], int, int)}.
     */
    @Test
    public void testWriteAtLongByteArrayIntIntErrors() {
        // First fill the test stream with zero data.
        for (int i = 0; i < 100; ++i) {
            myTestStream.write(0);
        }

        try {
            myTestStream.writeAt(0, null, 0, 0);
            fail("Should have thrown.");
        } catch (final NullPointerException npe) {
            // Good.
        }

        try {
            myTestStream.writeAt(0, new byte[1], -1, 0);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.writeAt(0, new byte[1], 3, 0);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.writeAt(0, new byte[1], 0, -1);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.writeAt(0, new byte[1], 1, 1);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.writeAt(99, new byte[10], 0, 2);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }
        try {
            myTestStream.writeAt(0, new byte[1], 1, Integer.MAX_VALUE);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

    }

    /**
     * Test method for
     * {@link RandomAccessOutputStream#writeAt(long, byte[], int, int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteAtLongByteArrayIntIntLengthZero() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final Random rand = new Random(System.currentTimeMillis());

        // First fill the test stream with random data.
        for (int i = 0; i < (16700 + 256); ++i) {
            myTestStream.write(0);
            bOut.write(0);
        }

        // Now copy over a segment at a time.
        final byte[] fullBuffer = bOut.toByteArray();
        int toCopy = fullBuffer.length;
        while (toCopy > 0) {
            final int copy = rand.nextInt(toCopy) + 1;
            final int start = fullBuffer.length - toCopy;

            myTestStream.writeAt(start, fullBuffer, start, 0);

            toCopy -= copy;
        }

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }

    /**
     * Test method for {@link RandomAccessOutputStream#writeAt(long, int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteAtLongInt() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final Random rand = new Random(System.currentTimeMillis());

        // First fill the test stream with random data.
        for (int i = 0; i < (4096 + 256); ++i) {
            myTestStream.write(0);
        }

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
            myTestStream.writeAt(i, i);
        }

        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
            myTestStream.writeAt(i + 256, value);
        }

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }

    /**
     * Test method for {@link RandomAccessOutputStream#write(byte[], int, int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteByteArray() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        final Random rand = new Random(System.currentTimeMillis());

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
        }
        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
        }

        myTestStream.write(bOut.toByteArray());

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }

    /**
     * Test method for {@link RandomAccessOutputStream#write(byte[], int, int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteByteArrayIntInt() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        final Random rand = new Random(System.currentTimeMillis());

        // First 0-255.
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
        }
        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
        }

        final int offset = rand.nextInt(2048);
        final int length = rand.nextInt(2048);
        myTestStream.write(bOut.toByteArray(), offset, length);

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals(
                "Byte arrays are not the same.",
                Arrays.copyOfRange(bOut.toByteArray(), offset, offset + length),
                finalOut.toByteArray());
    }

    /**
     * Test method for {@link RandomAccessOutputStream#write(byte[], int, int)}.
     */
    @Test
    public void testWriteByteArrayIntIntErrors() {
        try {
            myTestStream.write(null, 0, 0);
            fail("Should have thrown.");
        } catch (final NullPointerException npe) {
            // Good.
        }

        try {
            myTestStream.write(new byte[1], -1, 0);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.write(new byte[1], 3, 0);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.write(new byte[1], 0, -1);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.write(new byte[1], 1, 1);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }

        try {
            myTestStream.write(new byte[1], 1, Integer.MAX_VALUE);
            fail("Should have thrown.");
        } catch (final IndexOutOfBoundsException npe) {
            // Good.
        }
    }

    /**
     * Test method for {@link RandomAccessOutputStream#write(int)}.
     * 
     * @throws IOException
     *             On a failure writing the test results.
     */
    @Test
    public void testWriteInt() throws IOException {
        // Use a ByteArrayOutputStream as an oracle.
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        final Random rand = new Random(System.currentTimeMillis());

        // First 0-255.
        assertEquals("The position should start @ zero.", 0,
                myTestStream.getPosition());
        for (int i = 0; i < 256; ++i) {
            bOut.write(i);
            myTestStream.write(i);
            assertEquals("The position is wrong.", i + 1,
                    myTestStream.getPosition());
        }
        assertEquals("The size is wrong.", 256, myTestStream.getSize());

        // Now some random value.
        for (int i = 0; i < 4096; ++i) {
            final int value = rand.nextInt(256);
            bOut.write(value);
            myTestStream.write(value);
            assertEquals("The position is wrong.", i + 256 + 1,
                    myTestStream.getPosition());
        }

        final ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
        myTestStream.writeTo(finalOut);

        assertArrayEquals("Byte arrays are not the same.", bOut.toByteArray(),
                finalOut.toByteArray());
    }
}
