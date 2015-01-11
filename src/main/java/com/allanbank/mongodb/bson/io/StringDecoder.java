/*
 * #%L
 * StringDecoder.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * StringDecoder provides a decoder for byte arrays into strings that uses a
 * trie data structure to cache recurring strings.
 * <p>
 * This class is <b>not</b> thread safe.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public class StringDecoder {

    /** UTF-8 Character set for encoding strings. */
    /* package */final static Charset UTF8 = Charset.forName("UTF-8");

    /** A builder for the ASCII strings. */
    private final StringBuilder myBuilder = new StringBuilder(64);

    /** The cached decoded strings. */
    private final StringDecoderCache myCache;

    /**
     * Creates a new StringDecoder.
     */
    public StringDecoder() {
        this(new StringDecoderCache());
    }

    /**
     * Creates a new StringDecoder.
     *
     * @param cache
     *            The cache for the decoder.
     */
    public StringDecoder(final StringDecoderCache cache) {
        super();

        myCache = cache;
    }

    /**
     * Decode a string of a known length. The last byte should be a zero byte
     * and will not be included in the decoded string.
     *
     * @param source
     *            The source of the bytes in the string.
     * @param offset
     *            The offset of the first byte to decode.
     * @param length
     *            The length of the string to decode with a terminal zero byte.
     * @return The decoded string.
     * @throws StreamCorruptedException
     *             On the decoding of the string failing.
     * @throws EOFException
     *             On the array not containing enough bytes to decoded.
     */
    public String decode(final byte[] source, final int offset, final int length)
            throws StreamCorruptedException, EOFException {

        String result = myCache.find(source, offset, length);
        if (result == null) {
            result = fastDecode(source, offset, length - 1);
        }

        myCache.used(result, source, offset, length);

        return result;
    }

    /**
     * Returns the cache value.
     *
     * @return The cache value.
     * @deprecated The cache {@link StringDecoderCache} should be controlled
     *             directly. This method will be removed after the 2.1.0
     *             release.
     */
    @Deprecated
    public StringDecoderCache getCache() {
        return myCache;
    }

    /**
     * Retrieves or caches the decoded string for the Trie.
     *
     * @param source
     *            The source of the bytes in the string.
     * @param offset
     *            The offset of the first byte to decode.
     * @param length
     *            The length of the string to decode without a terminal zero
     *            byte.
     * @return The value for the string.
     */
    private String fastDecode(final byte[] source, final int offset,
            final int length) {
        // Try to decode as ASCII.
        boolean isAscii = true;
        for (int i = 0; isAscii && (i < length); ++i) {
            final int b = (source[offset + i] & 0xFF);
            if (b < 0x80) {
                myBuilder.append((char) b);
            }
            else {
                isAscii = false;
            }
        }

        String result;
        if (!isAscii) {
            final int encodedLength = myBuilder.length();

            final String remaining = new String(source, offset + encodedLength,
                    length - encodedLength, UTF8);

            myBuilder.append(remaining);
        }
        result = myBuilder.toString();

        // Clear the string builder.
        myBuilder.setLength(0);

        return result;
    }
}
