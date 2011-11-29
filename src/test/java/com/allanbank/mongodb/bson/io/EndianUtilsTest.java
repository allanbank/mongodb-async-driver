/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

/**
 * Test class for the {@link EndianUtils} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class EndianUtilsTest {

    /**
     * Test method for
     * {@link com.allanbank.mongodb.bson.io.EndianUtils#swap(int)}.
     */
    @Test
    public void testSwapInt() {
        for (int i = -5; i < 5; ++i) {
            final int swapped = EndianUtils.swap(i);
            if ((i != 0) && (i != -1)) {
                assertTrue("The swaped value should not equals the original i="
                        + i + ", swapped=" + swapped, i != swapped);
            }
            assertEquals("Double swap should return the same value.", i,
                    EndianUtils.swap(swapped));
        }

        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 100; ++i) {
            final int value = random.nextInt();
            final int swapped = EndianUtils.swap(value);
            assertEquals("Double swap should return the same value value="
                    + value + ", swapped=" + swapped, value,
                    EndianUtils.swap(swapped));
        }
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.bson.io.EndianUtils#swap(long)}.
     */
    @Test
    public void testSwapLong() {
        for (long i = -5; i < 5; ++i) {
            final long swapped = EndianUtils.swap(i);
            if ((i != 0) && (i != -1)) {
                assertTrue("The swaped value should not equals the original.",
                        i != swapped);
            }
            assertEquals("Double swap should return the same value.", i,
                    EndianUtils.swap(swapped));
        }

        final Random random = new Random(System.currentTimeMillis());
        for (long i = 0; i < 100; ++i) {
            final long value = random.nextInt();
            final long swapped = EndianUtils.swap(value);
            assertEquals("Double swap should return the same value.", value,
                    EndianUtils.swap(swapped));
        }
    }

}
