/*
 * #%L
 * StringEncoderTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * StringEncoderTest provides tests for the {@link StringEncoder} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringEncoderTest {

    /**
     * Test method for {@link StringEncoder#computeCStringSize(String)}.
     */
    @Test
    public void testComputeCStringSize() {
        assertThat(StringEncoder.computeCStringSize("abc"), is(3 + 1));
        assertThat(StringEncoder.computeCStringSize(""), is(0 + 1));

        assertThat(StringEncoder.computeCStringSize("\u0079"), is(1 + 1));
        assertThat(StringEncoder.computeCStringSize("\u0080"), is(2 + 1));
        assertThat(StringEncoder.computeCStringSize("\u0081"), is(2 + 1));

        assertThat(StringEncoder.computeCStringSize("\u0799"), is(2 + 1));
        assertThat(StringEncoder.computeCStringSize("\u0800"), is(3 + 1));
        assertThat(StringEncoder.computeCStringSize("\u0801"), is(3 + 1));

        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x10000 - 1))), is(3 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x10000))), is(4 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x10000 + 1))), is(4 + 1));

        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x40000 - 1))), is(4 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x40000))), is(4 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x40000 + 1))), is(4 + 1));

        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x100000 - 1))), is(4 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x100000))), is(4 + 1));
        assertThat(StringEncoder.computeCStringSize(String.valueOf(Character
                .toChars(0x100000 + 1))), is(4 + 1));
    }

    /**
     * Test method for {@link StringEncoder#computeStringSize(String)}.
     */
    @Test
    public void testComputeStringSize() {
        assertThat(StringEncoder.computeStringSize("abc"), is(3 + 5));
        assertThat(StringEncoder.computeStringSize(""), is(0 + 5));

        assertThat(StringEncoder.computeStringSize("\u0079"), is(1 + 5));
        assertThat(StringEncoder.computeStringSize("\u0080"), is(2 + 5));
        assertThat(StringEncoder.computeStringSize("\u0081"), is(2 + 5));

        assertThat(StringEncoder.computeStringSize("\u0799"), is(2 + 5));
        assertThat(StringEncoder.computeStringSize("\u0800"), is(3 + 5));
        assertThat(StringEncoder.computeStringSize("\u0801"), is(3 + 5));

        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x10000 - 1))), is(3 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x10000))), is(4 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x10000 + 1))), is(4 + 5));

        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x40000 - 1))), is(4 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x40000))), is(4 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x40000 + 1))), is(4 + 5));

        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x100000 - 1))), is(4 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x100000))), is(4 + 5));
        assertThat(StringEncoder.computeStringSize(String.valueOf(Character
                .toChars(0x100000 + 1))), is(4 + 5));
    }

    /**
     * Test method for
     * {@link StringEncoder#encode(String, java.io.OutputStream)}.
     * 
     * @throws IOException
     *             On a failure encoding the string.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testEncode() throws IOException {
        final StringEncoder encoder = new StringEncoder();

        assertThat(encoder.getCache(), notNullValue());

        // Repeat the tests to engage the cache.
        for (int i = 0; i < (AbstractStringCache.MAX_MULTIPLIER * AbstractStringCache.DEFAULT_MAX_CACHE_ENTRIES); ++i) {
            check(encoder, "abc", new byte[] { (byte) 'a', (byte) 'b',
                    (byte) 'c' });
            check(encoder, "", new byte[] {});

            check(encoder, "\u0079", new byte[] { (byte) 121 });
            check(encoder, "\u0080", new byte[] { (byte) -62, (byte) -128 });
            check(encoder, "\u0081", new byte[] { (byte) -62, (byte) -127 });

            check(encoder, "\u0799", new byte[] { (byte) -34, (byte) -103 });
            check(encoder, "\u0800", new byte[] { (byte) -32, (byte) -96,
                    (byte) -128 });
            check(encoder, "\u0801", new byte[] { (byte) -32, (byte) -96,
                    (byte) -127 });

            check(encoder, String.valueOf(Character.toChars(0x10000 - 1)),
                    new byte[] { (byte) -17, (byte) -65, (byte) -65 });
            check(encoder, String.valueOf(Character.toChars(0x10000)),
                    new byte[] { (byte) -16, (byte) -112, (byte) -128,
                            (byte) -128 });
            check(encoder, String.valueOf(Character.toChars(0x10000 + 1)),
                    new byte[] { (byte) -16, (byte) -112, (byte) -128,
                            (byte) -127 });

            check(encoder,
                    String.valueOf(Character.toChars(0x40000 - 1)),
                    new byte[] { (byte) -16, (byte) -65, (byte) -65, (byte) -65 });
            check(encoder, String.valueOf(Character.toChars(0x40000)),
                    new byte[] { (byte) -15, (byte) -128, (byte) -128,
                            (byte) -128 });
            check(encoder, String.valueOf(Character.toChars(0x40000 + 1)),
                    new byte[] { (byte) -15, (byte) -128, (byte) -128,
                            (byte) -127 });

            check(encoder,
                    String.valueOf(Character.toChars(0x100000 - 1)),
                    new byte[] { (byte) -13, (byte) -65, (byte) -65, (byte) -65 });
            check(encoder, String.valueOf(Character.toChars(0x100000)),
                    new byte[] { (byte) -12, (byte) -128, (byte) -128,
                            (byte) -128 });
            check(encoder, String.valueOf(Character.toChars(0x100000 + 1)),
                    new byte[] { (byte) -12, (byte) -128, (byte) -128,
                            (byte) -127 });

            // Check size encoding with a hot cache.
            assertThat(encoder.encodeSize("abc"), is(3));
        }
    }

    /**
     * Test method for {@link StringEncoder#encodeSize(String)}.
     */
    @Test
    public void testEncodeSize() {
        final StringEncoder encoder = new StringEncoder();

        assertThat(encoder.encodeSize("abc"), is(3));
        assertThat(encoder.encodeSize(""), is(0));

        assertThat(encoder.encodeSize("\u0079"), is(1));
        assertThat(encoder.encodeSize("\u0080"), is(2));
        assertThat(encoder.encodeSize("\u0081"), is(2));

        assertThat(encoder.encodeSize("\u0799"), is(2));
        assertThat(encoder.encodeSize("\u0800"), is(3));
        assertThat(encoder.encodeSize("\u0801"), is(3));

        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x10000 - 1))), is(3));
        assertThat(
                encoder.encodeSize(String.valueOf(Character.toChars(0x10000))),
                is(4));
        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x10000 + 1))), is(4));

        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x40000 - 1))), is(4));
        assertThat(
                encoder.encodeSize(String.valueOf(Character.toChars(0x40000))),
                is(4));
        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x40000 + 1))), is(4));

        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x100000 - 1))), is(4));
        assertThat(
                encoder.encodeSize(String.valueOf(Character.toChars(0x100000))),
                is(4));
        assertThat(encoder.encodeSize(String.valueOf(Character
                .toChars(0x100000 + 1))), is(4));
    }

    /**
     * Test method for {@link StringEncoder#utf8Size(String)}.
     */
    @Test
    public void testUtf8Size() {
        assertThat(StringEncoder.utf8Size("abc"), is(3));
        assertThat(StringEncoder.utf8Size(""), is(0));

        assertThat(StringEncoder.utf8Size("\u0079"), is(1));
        assertThat(StringEncoder.utf8Size("\u0080"), is(2));
        assertThat(StringEncoder.utf8Size("\u0081"), is(2));

        assertThat(StringEncoder.utf8Size("\u0799"), is(2));
        assertThat(StringEncoder.utf8Size("\u0800"), is(3));
        assertThat(StringEncoder.utf8Size("\u0801"), is(3));

        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x10000 - 1))), is(3));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x10000))), is(4));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x10000 + 1))), is(4));

        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x40000 - 1))), is(4));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x40000))), is(4));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x40000 + 1))), is(4));

        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x100000 - 1))), is(4));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x100000))), is(4));
        assertThat(StringEncoder.utf8Size(String.valueOf(Character
                .toChars(0x100000 + 1))), is(4));
    }

    /**
     * Checks the encoding of a string matches the expected value.
     * 
     * @param encoder
     *            The encoder to use in the test.
     * @param string
     *            The String to encode.
     * @param expected
     *            The expected encoding.
     * @throws IOException
     *             On a failure encoding the string.
     */
    private void check(final StringEncoder encoder, final String string,
            final byte[] expected) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        encoder.encode(string, out);

        assertThat(out.toByteArray(), is(expected));
    }

}
