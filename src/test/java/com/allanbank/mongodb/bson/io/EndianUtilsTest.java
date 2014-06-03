/*
 * #%L
 * EndianUtilsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
