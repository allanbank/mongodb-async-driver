/*
 * #%L
 * AbstractStringCache.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractStringCache provides the basic functionality for tracking string
 * usage and triggering rebuilds of the cache over time.
 * <p>
 * The basic function of the cache is to maintain two structures. The first
 * (provided by this class) tracks the usage of strings in the cache via a
 * 'seen' {@link ConcurrentMap}. As the cache sees more usage it will
 * periodically trim entries from the map that are not being used often (this is
 * to limit memory usage). Once the cache thinks it has seen enough usage it
 * will trigger the building of the runtime cache. This runtime cache does not
 * require any locking as it is read-only after being constructed.
 * </p>
 * <p>
 * There are two controls on the amount of caching instances of this class will
 * do:
 * </p>
 * <ol>
 * <li>
 * {@link #getMaxCacheLength()} limits the length of the encoded bytes this
 * class will try and retrieve from the cache. Setting this value to zero
 * disables the cache and limit any memory overhead.</li>
 * <li>{@link #getMaxCacheEntries()} controls the number of cached entries in
 * the runtime-cache and how often the accumulated statistics are trimmed. Each
 * entry represents a single encoded string.</li>
 * </ol>
 * 
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractStringCache {

    /** The default maximum number of entries to keep in the cache. */
    public static final int DEFAULT_MAX_CACHE_ENTRIES = 24;

    /** The default maximum length byte array to cache. */
    public static final int DEFAULT_MAX_CACHE_LENGTH = 25;

    /** The maximum value for the multiplier. */
    protected static final int MAX_MULTIPLIER = 100;

    /**
     * The maximum length of a string to cache. This can be used to stop a
     * single long string from pushing useful values out of the cache.
     */
    protected int myMaxCacheLength;

    /** The maximum number of entries to have in the cache. */
    protected int myMaxCachEntries;

    /**
     * The multiplier for the number of times the cache is used before we
     * re-load the cache.
     */
    protected volatile int myMaxCachEntriesMultiplier;

    /**
     * The map of seen strings that will be used periodically to build the cache
     * cache.
     */
    protected final ConcurrentMap<String, SeenString> mySeen;

    /**
     * The number of time the cache has been used since the last re-load of
     * runtime, read-only cache.
     */
    protected final AtomicInteger myUseCount;

    /**
     * Creates a new AbstractStringCache.
     */
    public AbstractStringCache() {
        super();

        mySeen = new ConcurrentHashMap<String, SeenString>(1024);

        myMaxCacheLength = DEFAULT_MAX_CACHE_LENGTH;
        myMaxCachEntries = DEFAULT_MAX_CACHE_ENTRIES;

        myMaxCachEntriesMultiplier = 0;
        myUseCount = new AtomicInteger(0);
    }

    /**
     * Returns the maximum node count.
     * 
     * @return The maximum node count.
     */
    public int getMaxCacheEntries() {
        return myMaxCachEntries;
    }

    /**
     * Returns the maximum length of strings to cache. This can be used to stop
     * a single long string from pushing useful values out of the cache.
     * 
     * @return The maximum length of strings to cache.
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
     * Sets the value of maximum length of strings to cache to the new value.
     * This can be used to stop a single long string from pushing useful values
     * out of the cache.
     * 
     * @param maxlength
     *            The new value for the maximum length of strings to cache.
     */
    public void setMaxCacheLength(final int maxlength) {
        myMaxCacheLength = maxlength;

        // The user has turned the cache off. Release all of the memory.
        if (maxlength <= 0) {
            clear();
        }
    }

    /**
     * Notification that a string/byte[] have been used.
     * 
     * @param source
     *            The bytes in the string.
     * @param offset
     *            The offset of the first byte.
     * @param length
     *            The length of the bytes with a terminal zero byte.
     * @param decoded
     *            The decoded string.
     */
    public void used(final String decoded, final byte[] source,
            final int offset, final int length) {

        // Check for the easiest value to cache or entries that are too long.
        if ((length <= 1) || (myMaxCacheLength < length)) {
            return;
        }

        SeenString entry = mySeen.get(decoded);
        if (entry == null) {
            entry = new SeenString(source, offset, length, decoded);

            final SeenString existing = mySeen.putIfAbsent(decoded, entry);
            if (existing != null) {
                entry = existing;
            }
        }

        // Count the use.
        entry.incrementCount();

        // Rebuild the count after period of use.
        int use = myUseCount.incrementAndGet();
        while ((myMaxCachEntries * myMaxCachEntriesMultiplier) < use) {
            use = tryRebuild(use);
        }

        // Periodically trim low count entries.
        if ((use != 0) && ((use % myMaxCachEntries) == 0)) {
            trimPending();
        }
    }

    /**
     * Builds a map of the seen strings at each count. The order of the map is
     * reversed so that higher counts are first in the map.
     * 
     * @return A map of the seen strings at each count.
     */
    protected SortedMap<Integer, List<SeenString>> buildCacheGroups() {
        myMaxCachEntriesMultiplier = Math.min(myMaxCachEntriesMultiplier + 1,
                MAX_MULTIPLIER);

        // Copy the current entries.
        final SortedMap<Integer, List<SeenString>> order = new TreeMap<Integer, List<SeenString>>(
                ReverseIntegerComparator.INSTANCE);

        for (final SeenString seen : mySeen.values()) {
            final Integer seenCount = Integer.valueOf(seen.reset());
            List<SeenString> seenAtCount = order.get(seenCount);
            if (seenAtCount == null) {
                seenAtCount = new LinkedList<SeenString>();
                order.put(seenCount, seenAtCount);
            }

            seenAtCount.add(seen);
        }

        return order;
    }

    /**
     * Clears the cache.
     */
    protected void clear() {
        mySeen.clear();
        myUseCount.set(0);
    }

    /**
     * Rebuilds the cache from the current {@link #mySeen} map. By the end of
     * the method the caceh should have been rebuild and the {@link #mySeen} map
     * cleared.
     */
    protected abstract void rebuildCache();

    /**
     * Trims the seen map of any entries that have not accumulated many counts.
     * This is to avoid the seen map holding too many entries.
     */
    private void trimPending() {
        // Only keep entries that have more than 1% (or more than 1 if 1% is <
        // 1) entries.
        final int trimLevel = Math.max(1, (int) (myMaxCachEntries * 0.01));
        int toRemove = Math.max(0, mySeen.size() - myMaxCachEntries);

        final Iterator<SeenString> iter = mySeen.values().iterator();
        while (iter.hasNext() && (0 < toRemove)) {
            final SeenString seen = iter.next();
            if (seen.getCount() <= trimLevel) {
                iter.remove();
                toRemove -= 1;
            }
        }
    }

    /**
     * Attempts to rebuild the cache.
     * 
     * @param use
     *            The expected use count.
     * @return The use count after the attempt.
     */
    private int tryRebuild(final int use) {
        if (myUseCount.compareAndSet(use, 0)) {
            rebuildCache();

            return 0;
        }
        return myUseCount.get();
    }

    /**
     * ReverseIntegerComparator provides a {@link Comparator} that considers
     * higher value integers to be less then lower value integers.
     * 
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class ReverseIntegerComparator implements
            Comparator<Integer>, Serializable {

        /** The single instance of the comparator. */
        public static final Comparator<Integer> INSTANCE = new ReverseIntegerComparator();

        /** The serialization version for the class. */
        private static final long serialVersionUID = 7342816815375930353L;

        /**
         * Creates a new ReverseIntegerComparator. Private since this class is
         * stateless and we only need a single copy.
         */
        private ReverseIntegerComparator() {
            super();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to reverse the comparison of integers.
         * </p>
         */
        @Override
        public int compare(final Integer o1, final Integer o2) {
            final int value = o1.compareTo(o2);

            if (value < 0) {
                return 1;
            }
            else if (0 < value) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }
}