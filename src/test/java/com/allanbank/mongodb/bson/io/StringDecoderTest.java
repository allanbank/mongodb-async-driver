/*
 * #%L
 * StringDecoderTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StreamCorruptedException;

import org.junit.Test;

/**
 * StringDecoderTest provides tests for the {@link StringDecoder}.
 * 
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringDecoderTest {

    /**
     * Test method for {@link StringDecoder#decode(byte[], int, int)}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testDecode() throws IOException {

        final StringDecoder decoder = new StringDecoder();

        // Short circuits.
        assertThat(decoder.decode(null, 0, 0), is(""));
        assertThat(decoder.decode(null, 0, 1), is(""));

        // Simple.
        byte[] data = new byte[] { 'a', 0 };
        String result = decoder.decode(data, 0, data.length);
        assertThat(result, is("a"));
        // And cached...
        for (int i = 0; i < AbstractStringCache.MAX_MULTIPLIER; ++i) {
            result = decoder.decode(data, 0, data.length);
        }
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // A bit longer.
        data = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 0 };
        result = decoder.decode(data, 0, data.length);
        assertThat(result, is("abcdefg"));
        // And cached...
        for (int i = 0; i < AbstractStringCache.MAX_MULTIPLIER; ++i) {
            result = decoder.decode(data, 0, data.length);
        }
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // A bit different.
        data = new byte[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 0 };
        result = decoder.decode(data, 0, data.length);
        assertThat(result, is("ABCDEFG"));
        // And cached...
        for (int i = 0; i < AbstractStringCache.MAX_MULTIPLIER; ++i) {
            result = decoder.decode(data, 0, data.length);
        }
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // Non-ascii
        data = "ABCDEF\u00ff\u0000".getBytes(StringDecoder.UTF8);
        result = decoder.decode(data, 0, data.length);
        assertThat(result, is("ABCDEF\u00ff"));
        // And cached...
        for (int i = 0; i < AbstractStringCache.MAX_MULTIPLIER; ++i) {
            result = decoder.decode(data, 0, data.length);
        }
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));
    }

    /**
     * Test method for {@link StringDecoder#decode(byte[], int, int)}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test(expected = StreamCorruptedException.class)
    public void testDecodeThrowsIfNoTerminalNull() throws IOException {

        final StringDecoder decoder = new StringDecoder();

        final byte[] data = new byte[] { 'a', 'b' };
        decoder.decode(data, 0, data.length);
    }
}
