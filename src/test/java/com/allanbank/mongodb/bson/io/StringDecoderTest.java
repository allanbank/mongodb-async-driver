/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.io;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * StringDecoderTest provides tests for the {@link StringDecoder}.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
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
        assertThat(decoder.getMaxCacheLength(),
                is(StringDecoder.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(decoder.getMaxCacheEntries(),
                is(StringDecoder.DEFAULT_MAX_CACHE_ENTRIES));

        // Short circuits.
        assertThat(decoder.decode(null, 0, 0), is(""));
        assertThat(decoder.decode(null, 0, 1), is(""));

        // Simple.
        byte[] data = new byte[] { 'a', 0 };
        String result = decoder.decode(data, 0, data.length);
        assertThat(result, is("a"));
        // And cached...
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // A bit longer.
        data = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 0 };
        result = decoder.decode(data, 0, data.length);
        assertThat(result, is("abcdefg"));
        // And cached...
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // A bit different.
        data = new byte[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 0 };
        result = decoder.decode(data, 0, data.length);
        assertThat(result, is("ABCDEFG"));
        // And cached...
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

    /**
     * Test method for {@link StringDecoder#getMaxCacheLength()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetMaxDepth() throws IOException {
        final StringDecoder decoder = new StringDecoder();
        assertThat(decoder.getMaxCacheLength(),
                is(StringDecoder.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(decoder.getMaxCacheEntries(),
                is(StringDecoder.DEFAULT_MAX_CACHE_ENTRIES));

        // Simple.
        final byte[] data = new byte[] { 'a', 0 };
        final String result = decoder.decode(data, 0, data.length);
        assertThat(result, is("a"));
        // And cached...
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result));

        // Turn off the cache.
        decoder.setMaxCacheLength(0);

        // Should not return the same value but still the right value.
        assertThat(decoder.decode(data, 0, data.length),
                not(sameInstance(result)));
        assertThat(decoder.decode(data, 0, data.length), is("a"));

        // Turn the cache back on...
        decoder.setMaxCacheLength(data.length);

        // And we are back to caching but not the original value still. (e.g.,
        // the cache was cleared before)
        final String result2 = decoder.decode(data, 0, data.length);
        assertThat(result2, not(sameInstance(result)));
        assertThat(result2, is("a"));
        // And cached...
        assertThat(decoder.decode(data, 0, data.length), sameInstance(result2));

    }

    /**
     * Test method for {@link StringDecoder#getMaxCacheEntries()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetMaxNodeCount() throws IOException {
        final StringDecoder decoder = new StringDecoder();
        assertThat(decoder.getMaxCacheLength(),
                is(StringDecoder.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(decoder.getMaxCacheEntries(),
                is(StringDecoder.DEFAULT_MAX_CACHE_ENTRIES));

        // Set the number of entries to 26 * 2.
        decoder.setMaxCacheEntries(26 * 2);

        // Decode 512 bytes of data...
        final List<String> decoded = new ArrayList<String>();
        final byte[] data = new byte[3];
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                data[0] = (byte) i;
                data[1] = (byte) j;

                decoded.add(decoder.decode(data, 0, data.length));
            }
        }

        // And we should hold all of those in the cache.
        int index = 0;
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                data[0] = (byte) i;
                data[1] = (byte) j;

                assertThat(decoder.decode(data, 0, data.length),
                        sameInstance(decoded.get(index)));

                index += 1;
            }
        }

        // Now add another entry.
        final byte[] extra = new byte[] { 'a', 0 };
        final String extraDecoded = decoder.decode(extra, 0, extra.length);
        assertThat(extraDecoded, is("a"));

        // Everyone but the first node should be in the cache.
        index = 0;
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                if (index != 0) {
                    data[0] = (byte) i;
                    data[1] = (byte) j;

                    assertThat(decoder.decode(data, 0, data.length),
                            sameInstance(decoded.get(index)));
                }
                index += 1;
            }
        }

        // And so should the extra entry.
        assertThat(decoder.decode(extra, 0, extra.length),
                sameInstance(extraDecoded));

        // Now if we access the original set in order each member should be
        // kicked out in order.
        index = 0;
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                data[0] = (byte) i;
                data[1] = (byte) j;

                assertThat(decoder.decode(data, 0, data.length),
                        not(sameInstance(decoded.get(index))));

                index += 1;
            }
        }

    }

}
