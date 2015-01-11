/*
 * #%L
 * StringDecoderCacheTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * StringDecoderCacheTest provides tests for the {@link StringDecoderCache}.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringDecoderCacheTest {

    /**
     * Test method for {@link StringDecoderCache#find(byte[], int, int)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test(expected = StreamCorruptedException.class)
    public void testDecodeThrowsIfNoTerminalNull() throws IOException {

        final StringDecoderCache cache = new StringDecoderCache();

        final byte[] data = new byte[] { 'a', 'b' };
        cache.find(data, 0, data.length);
    }

    /**
     * Test method for {@link StringDecoderCache#find(byte[], int, int)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testFind() throws IOException {

        final StringDecoderCache cache = new StringDecoderCache();
        assertThat(cache.getMaxCacheLength(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(cache.getMaxCacheEntries(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_ENTRIES));

        // Short circuits.
        assertThat(cache.find(null, 0, 0), is(""));
        assertThat(cache.find(null, 0, 1), is(""));

        // Simple.
        final byte[] data = new byte[] { 'b', 0 };
        assertThat(cache.find(data, 0, data.length), nullValue());
        // And still not cached...
        assertThat(cache.find(data, 0, data.length), nullValue());

        // Lets get some stuff cached.
        cache.setMaxCacheEntries(10);
        final byte[] a = new byte[] { 'a', 0 };
        final byte[] abcdefg = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g',
                0 };
        final byte[] aBCDEFG = new byte[] { 'a', 'B', 'C', 'D', 'E', 'F', 'G',
                0 };
        for (int i = 0; i < (10 * AbstractStringCache.MAX_MULTIPLIER); ++i) {
            cache.used("a", a, 0, a.length);
            cache.used("abcdefg", abcdefg, 0, abcdefg.length);
            cache.used("aBCDEFG", aBCDEFG, 0, aBCDEFG.length);
        }

        // Now get the cached results.
        String result = cache.find(a, 0, a.length);
        assertThat(result, is("a"));
        // And cached...
        assertThat(cache.find(a, 0, a.length), sameInstance(result));

        // Different.
        result = cache.find(abcdefg, 0, abcdefg.length);
        assertThat(result, is("abcdefg"));
        // And cached...
        assertThat(cache.find(abcdefg, 0, abcdefg.length), sameInstance(result));

        // A bit different.
        result = cache.find(aBCDEFG, 0, abcdefg.length);
        assertThat(result, is("aBCDEFG"));
        // And cached...
        assertThat(cache.find(aBCDEFG, 0, abcdefg.length), sameInstance(result));

        // Should not find.
        assertThat(cache.find(data, 0, data.length), nullValue());
    }

    /**
     * Test method for {@link StringDecoderCache#getMaxCacheLength()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetMaxDepth() throws IOException {
        final StringDecoderCache cache = new StringDecoderCache();
        assertThat(cache.getMaxCacheLength(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(cache.getMaxCacheEntries(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_ENTRIES));

        cache.setMaxCacheEntries(2);

        // Simple.
        final byte[] data = new byte[] { 'a', 0 };
        for (int iteration = 0; iteration < AbstractStringCache.MAX_MULTIPLIER; ++iteration) {
            cache.used("a", data, 0, data.length);
        }
        final String result = cache.find(data, 0, data.length);
        assertThat(result, is("a"));
        // And cached...
        assertThat(cache.find(data, 0, data.length), sameInstance(result));

        // Turn off the cache.
        cache.setMaxCacheLength(0);

        // Should not return a value.
        assertThat(cache.find(data, 0, data.length), nullValue());
        assertThat(cache.find(data, 0, data.length), nullValue());

        // Turn the cache back on...
        cache.setMaxCacheLength(data.length);

        // And we are back to caching but not the original value still. (e.g.,
        // the cache was cleared before)
        final String result2 = cache.find(data, 0, data.length);
        assertThat(result2, nullValue());
        for (int iteration = 0; iteration < AbstractStringCache.MAX_MULTIPLIER; ++iteration) {
            cache.used("a", data, 0, data.length);
        }

        // And cached...
        final String result3 = cache.find(data, 0, data.length);
        assertThat(result3, is("a"));
        assertThat(cache.find(data, 0, data.length), sameInstance(result3));

    }

    /**
     * Test method for {@link StringDecoderCache#getMaxCacheEntries()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetMaxEntries() throws IOException {
        final StringDecoderCache cache = new StringDecoderCache();
        assertThat(cache.getMaxCacheLength(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_LENGTH));
        assertThat(cache.getMaxCacheEntries(),
                is(AbstractStringCache.DEFAULT_MAX_CACHE_ENTRIES));

        // Set the number of entries to 26 * 2.
        cache.setMaxCacheEntries(26 * 2);

        // Decode 512 bytes of data...
        final List<String> decoded = new ArrayList<String>();
        final StringBuilder string = new StringBuilder(2);
        for (int iteration = 0; iteration < AbstractStringCache.MAX_MULTIPLIER; ++iteration) {
            for (int i = 'a'; i <= 'b'; ++i) {
                for (int j = 'a'; j <= 'z'; ++j) {
                    final byte[] data = new byte[3];
                    data[0] = (byte) i;
                    data[1] = (byte) j;

                    string.setLength(0);
                    string.append((char) i);
                    string.append((char) j);

                    final String value = string.toString();

                    if (iteration == 0) {
                        decoded.add(value);
                    }

                    cache.used(value, data, 0, data.length);
                }
            }
        }

        // And we should hold all of those in the cache.
        int index = 0;
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                final byte[] data = new byte[3];
                data[0] = (byte) i;
                data[1] = (byte) j;

                assertThat(cache.find(data, 0, data.length),
                        is(decoded.get(index)));

                index += 1;
            }
        }

        // Now add another entry.
        final byte[] extra = new byte[] { 'a', 0 };
        final String extraDecoded = "a";
        for (int i = 0; i < cache.getMaxCacheEntries(); ++i) {
            cache.used(extraDecoded, extra, 0, extra.length);
        }

        // Everyone should still be in the cache.
        index = 0;
        for (int i = 'a'; i <= 'b'; ++i) {
            for (int j = 'a'; j <= 'z'; ++j) {
                final byte[] data = new byte[3];
                data[0] = (byte) i;
                data[1] = (byte) j;

                assertThat(cache.find(data, 0, data.length),
                        is(decoded.get(index)));
                index += 1;
            }
        }

        // But not the brand new entry.
        assertThat(cache.find(extra, 0, extra.length), nullValue());

        // Force a rebuild of the cache.
        cache.rebuildCache();

        // Now the new entry should be in the cache.
        assertThat(cache.find(extra, 0, extra.length), is("a"));
    }
}
