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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * StringEncoderCache provides the ability to cache the encoding of a string to
 * speed the writing of strings.
 * <p>
 * This class is thread safe. Thread safety is achieved by maintaining two data
 * structures. The first is a map of seen strings to the number of times the
 * string has been seen. The map is maintained by the base class:
 * {@link AbstractStringCache}. The second structure is a simple map of the
 * cached {@link String} to the encoded {@code byte[]}. The map has no locking
 * or synchronization since it is read-only after construction.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public class StringEncoderCache
        extends AbstractStringCache {

    /** The cache of strings to bytes. */
    private Map<String, byte[]> myCache;

    /**
     * Creates a new StringEncoder.
     */
    public StringEncoderCache() {
        myCache = Collections.emptyMap();

        myMaxCacheLength = DEFAULT_MAX_CACHE_LENGTH;
        myMaxCachEntries = DEFAULT_MAX_CACHE_ENTRIES;
    }

    /**
     * Looks in the cache for encoded bytes for the specified string.
     *
     * @param string
     *            The string value to find the cached bytes for.
     * @return The cached bytes for the string. May be <code>null</code>.
     */
    public byte[] find(final String string) {
        return myCache.get(string);
    }

    /**
     * Clears the cache.
     */
    @Override
    protected void clear() {
        myCache = Collections.emptyMap();
        super.clear();
    }

    /**
     * Rebuilds the cache from the current collection of seen entries.
     */
    @Override
    protected void rebuildCache() {
        final SortedMap<Integer, List<SeenString>> order = buildCacheGroups();

        // Rebuild the cache.
        int count = 0;
        final Map<String, byte[]> cache = new HashMap<String, byte[]>(
                (int) Math.ceil(Math.min(order.size(), myMaxCachEntries) / 0.75));
        for (final List<SeenString> seenAtCount : order.values()) {
            for (final SeenString seen : seenAtCount) {
                if (count < myMaxCachEntries) {
                    cache.put(seen.getValue(), seen.getBytes());
                    count += 1;
                }
                else {
                    mySeen.remove(seen.getValue());
                }
            }
        }

        myCache = cache;
    }
}
