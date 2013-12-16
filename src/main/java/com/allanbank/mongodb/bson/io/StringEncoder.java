/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * StringEncoder provides a single location for the string encoding and sizing
 * logic. This class if backed by a cache of strings to the encoded bytes.
 * <p>
 * The cache is controlled via two parameters:
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringEncoder {

    /** The default maximum number of strings to keep in the trie cache. */
    public static final int DEFAULT_MAX_CACHE_ENTRIES = 128;

    /** The default maximum length byte array to cache. */
    public static final int DEFAULT_MAX_CACHE_LENGTH = 25;

    /** The byte value limit for a ASCII character. */
    /* package */static final int ASCII_LIMIT = 0x80;

    /** An empty array of bytes. */
    /* package */static final byte[] EMPTY = new byte[0];

    /** The byte value limit for a two byte encoded characters. */
    /* package */static final int TWO_BYTE_LIMIT = 0x800;

    /** UTF-8 Character set for encoding strings. */
    /* package */final static Charset UTF8 = Charset.forName("UTF-8");

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
        final int strLength = string.length();

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

    /** The maximum number of strings to have in the cache. */
    protected int myMaxCachEntries;

    /** A private buffer for encoding strings. */
    private final byte[] myBuffer = new byte[1024];

    /** The cache of strings to bytes. */
    private final Map<String, byte[]> myCache;

    /**
     * The maximum length of a string to add to the cache. This can be used to
     * stop a long strings from pushing useful values out of the cache.
     */
    private int myMaxCacheLength;

    /**
     * Creates a new StringEncoder.
     */
    public StringEncoder() {
        myCache = new LinkedHashMap<String, byte[]>(10, 0.75f, true) {
            /** Serialization version for the class. */
            private static final long serialVersionUID = -3018944379241796804L;

            @Override
            protected boolean removeEldestEntry(
                    final Map.Entry<String, byte[]> eldest) {
                return size() > myMaxCachEntries;
            }
        };

        myMaxCacheLength = DEFAULT_MAX_CACHE_LENGTH;
        myMaxCachEntries = DEFAULT_MAX_CACHE_ENTRIES;
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
            byte[] encoded = myCache.get(string);
            if (encoded == null) {
                encoded = fastEncode(string, out);
                if (encoded.length != 0) {
                    myCache.put(string, encoded);
                }
            }
            else {
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

        final byte[] cached = myCache.get(string);
        if (cached != null) {
            return cached.length;
        }
        else if (string.length() < myMaxCacheLength) {
            try {
                final byte[] encoded = fastEncode(string, null);
                if (encoded.length != 0) {
                    return encoded.length;
                }
            }
            catch (final IOException ioe) {
                // No IO so should not happen. Fall through.
            }
        }
        return utf8Size(string);
    }

    /**
     * Returns the maximum number of strings that can be cached.
     * 
     * @return The maximum number of strings that can be cached.
     */
    public int getMaxCacheEntries() {
        return myMaxCachEntries;
    }

    /**
     * Returns the maximum length for a string that the encoder is allowed to
     * cache.
     * 
     * @return The maximum length for a string that the encoder is allowed to
     *         cache.
     */
    public int getMaxCacheLength() {
        return myMaxCacheLength;
    }

    /**
     * Sets the value of maximum number of cached strings.
     * 
     * @param maxCacheEntries
     *            The new value for the maximum number of cached strings.
     */
    public void setMaxCacheEntries(final int maxCacheEntries) {
        myMaxCachEntries = maxCacheEntries;
    }

    /**
     * Sets the value of length for a string that the encoder is allowed to
     * cache to the new value. This can be used to stop a single long string
     * from pushing useful values out of the cache.
     * 
     * @param maxlength
     *            The new value for the length for a string that the encoder is
     *            allowed to cache.
     */
    public void setMaxCacheLength(final int maxlength) {
        myMaxCacheLength = maxlength;

        // The user has turned the cache off. Release all of the memory.
        if (maxlength <= 0) {
            myCache.clear();
        }
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
     * @return The encoded bytes shorted that the internal buffer.
     * @throws IOException
     *             On a failure to write the bytes.
     */
    protected byte[] fastEncode(final String string, final OutputStream out)
            throws IOException {
        // 4 = max encoded bytes/code point.
        final int writeUpTo = myBuffer.length - 4;
        final int strLength = string.length();

        boolean returnBytes = (strLength <= myMaxCacheLength);

        int bufferOffset = 0;
        int codePoint;
        for (int i = 0; i < strLength; i += Character.charCount(codePoint)) {

            // Check for buffer overflow.
            if (writeUpTo < bufferOffset) {
                returnBytes = false;
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

        // ... and return a copy to cache.
        if (returnBytes) {
            return Arrays.copyOf(myBuffer, bufferOffset);
        }
        return EMPTY;
    }
}
