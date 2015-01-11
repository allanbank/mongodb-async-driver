/*
 * #%L
 * StringEncoder.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * StringEncoder provides a single location for the string encoding and sizing
 * logic. This class if backed by a cache of strings to the encoded bytes.
 * <p>
 * The cache is controlled via two parameters:
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public class StringEncoder {

    /**
     * Returns the visitor's output buffer.
     *
     * @param string
     *            The 'C' string to determine the size of.
     * @return The visitor's output buffer.
     */
    public static int computeCStringSize(final String string) {
        return utf8Size(string) + 1;
    }

    /**
     * Returns the visitor's output buffer.
     *
     * @param string
     *            The 'UTF8' string to determine the size of.
     * @return The visitor's output buffer.
     */
    public static int computeStringSize(final String string) {
        return 4 + utf8Size(string) + 1;
    }

    /**
     * Computes the size of the encoded UTF8 String based on the table below.
     *
     * <pre>
     * #    Code Points      Bytes
     * 1    U+0000..U+007F   1
     * 
     * 2    U+0080..U+07FF   2
     * 
     * 3    U+0800..U+0FFF   3
     *      U+1000..U+FFFF
     * 
     * 4   U+10000..U+3FFFF  4
     *     U+40000..U+FFFFF  4
     *    U+100000..U10FFFF  4
     * </pre>
     *
     * @param string
     *            The string to determine the length of.
     * @return The length of the string encoded as UTF8.
     */
    public static int utf8Size(final String string) {
        final int strLength = (string == null) ? 0 : string.length();

        int length = 0;
        int codePoint;
        for (int i = 0; i < strLength; i += Character.charCount(codePoint)) {
            codePoint = Character.codePointAt(string, i);
            if (codePoint < 0x80) {
                length += 1;
            }
            else if (codePoint < 0x800) {
                length += 2;
            }
            else if (codePoint < 0x10000) {
                length += 3;
            }
            else {
                length += 4;
            }
        }

        return length;
    }

    /** A private buffer for encoding strings. */
    private final byte[] myBuffer = new byte[1024];

    /** The cache of strings to bytes. */
    private final StringEncoderCache myCache;

    /**
     * Creates a new StringEncoder.
     */
    public StringEncoder() {
        this(new StringEncoderCache());
    }

    /**
     * Creates a new StringEncoder.
     *
     * @param cache
     *            The cache for the encoder to use.
     */
    public StringEncoder(final StringEncoderCache cache) {
        myCache = cache;
    }

    /**
     * Writes the string as a UTF-8 string. This method handles the
     * "normal/easy" cases and delegates to the full character set if things get
     * complicated.
     *
     * @param string
     *            The string to encode.
     * @param out
     *            The stream to write to.
     * @throws IOException
     *             On a failure to write the bytes.
     */
    public void encode(final String string, final OutputStream out)
            throws IOException {

        if (!string.isEmpty()) {
            final byte[] encoded = myCache.find(string);

            if (encoded == null) {
                // Cache miss - write the bytes straight to the stream.
                fastEncode(string, out);
            }
            else {
                myCache.used(string, encoded, 0, encoded.length);
                out.write(encoded);
            }
        }
    }

    /**
     * Computes the size of the encoded UTF8 String based on the table below.
     * This method may use a cached copy of the encoded string to determine the
     * size.
     *
     * <pre>
     * #    Code Points      Bytes
     * 1    U+0000..U+007F   1
     * 
     * 2    U+0080..U+07FF   2
     * 
     * 3    U+0800..U+0FFF   3
     *      U+1000..U+FFFF
     * 
     * 4   U+10000..U+3FFFF  4
     *     U+40000..U+FFFFF  4
     *    U+100000..U10FFFF  4
     * </pre>
     *
     * @param string
     *            The string to determine the length of.
     * @return The length of the string encoded as UTF8.
     */
    public int encodeSize(final String string) {
        if (string.isEmpty()) {
            return 0;
        }

        final byte[] cached = myCache.find(string);
        if (cached != null) {
            // Don't count this as a usage. Just bonus speed.
            return cached.length;
        }
        return utf8Size(string);
    }

    /**
     * Returns the cache value.
     *
     * @return The cache value.
     * @deprecated The cache {@link StringEncoderCache} should be controlled
     *             directly. This method will be removed after the 2.1.0
     *             release.
     */
    @Deprecated
    public StringEncoderCache getCache() {
        return myCache;
    }

    /**
     * Writes the string as a UTF-8 string. This method handles the
     * "normal/easy" cases and delegates to the full character set if things get
     * complicated.
     *
     * @param string
     *            The string to encode.
     * @param out
     *            The stream to write to.
     * @throws IOException
     *             On a failure to write the bytes.
     */
    protected void fastEncode(final String string, final OutputStream out)
            throws IOException {
        // 4 = max encoded bytes/code point.
        final int writeUpTo = myBuffer.length - 4;
        final int strLength = string.length();

        boolean bufferHasAllBytes = true;

        int bufferOffset = 0;
        int codePoint;
        for (int i = 0; i < strLength; i += Character.charCount(codePoint)) {

            // Check for buffer overflow.
            if (writeUpTo < bufferOffset) {
                bufferHasAllBytes = false;
                if (out != null) {
                    out.write(myBuffer, 0, bufferOffset);
                }
                bufferOffset = 0;
            }

            codePoint = Character.codePointAt(string, i);
            if (codePoint < 0x80) {
                myBuffer[bufferOffset++] = (byte) codePoint;
            }
            else if (codePoint < 0x800) {
                myBuffer[bufferOffset++] = (byte) (0xC0 + ((codePoint >> 6) & 0xFF));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 0) & 0x3F));
            }
            else if (codePoint < 0x10000) {
                myBuffer[bufferOffset++] = (byte) (0xE0 + ((codePoint >> 12) & 0xFF));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 6) & 0x3F));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 0) & 0x3f));
            }
            else {
                myBuffer[bufferOffset++] = (byte) (0xF0 + ((codePoint >> 18) & 0xFF));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 12) & 0x3F));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 6) & 0x3F));
                myBuffer[bufferOffset++] = (byte) (0x80 + ((codePoint >> 0) & 0x3F));
            }
        }

        // Write out the final results.
        if (out != null) {
            out.write(myBuffer, 0, bufferOffset);
        }

        // ... and try and save it in the cache.
        if (bufferHasAllBytes) {
            myCache.used(string, myBuffer, 0, bufferOffset);
        }
    }
}
