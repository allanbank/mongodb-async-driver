/*
 * #%L
 * StringDecoderCache.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.List;
import java.util.SortedMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * StringDecoderCache provides a cache for decoding strings.
 * <p>
 * This class is thread safe. Thread safety is achieved by maintaining two data
 * structures. The first is a map of seen strings to the number of times the
 * string has been seen. The map is maintained by the base class:
 * {@link AbstractStringCache}. The second structure is a trie of the cached
 * {@code byte[]} to the decoded string.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public class StringDecoderCache
        extends AbstractStringCache {

    /** The cached decoded strings in the form of a trie cache. */
    private TrieCache myTrieCache;

    /**
     * Creates a new TrieStringCache.
     */
    public StringDecoderCache() {
        super();

        myTrieCache = new TrieCache();
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
    public String find(final byte[] source, final int offset, final int length)
            throws StreamCorruptedException, EOFException {

        // Check for the easiest value to cache.
        if (length <= 1) {
            return "";
        }
        // And for values blowing out the cache.
        else if (myMaxCacheLength < length) {
            return null;
        }
        // Check for the terminal null.
        else if (source[(offset + length) - 1] != 0) {
            throw new StreamCorruptedException(
                    "Expected a null character at the end of the string.");
        }

        return myTrieCache.find(source, offset, length);
    }

    /**
     * Clears the cache.
     */
    @Override
    protected void clear() {
        myTrieCache = new TrieCache();
        super.clear();
    }

    /**
     * Rebuilds the cache from the current collection of seen entries.
     */
    @Override
    protected void rebuildCache() {
        final SortedMap<Integer, List<SeenString>> order = buildCacheGroups();

        // Rebuild the cache.
        final TrieCache cache = new TrieCache();
        int count = 0;
        for (final List<SeenString> seenAtCount : order.values()) {
            for (final SeenString seen : seenAtCount) {
                if (count < myMaxCachEntries) {
                    cache.addEntry(seen.getBytes(), seen.getValue());
                    count += 1;
                }
                else {
                    mySeen.remove(seen.getValue());
                }
            }
        }

        myTrieCache = cache;
    }

    /**
     * A fast cache for bytes to decoded strings.
     * <p>
     * The trie is designed to be used in a RCU fashion where the trie is
     * constructed as a series of {@link #addEntry(byte[], String)} calls and
     * once the trie is fully populated then is can be queried using
     * {@link #find(byte[], int, int)}.
     * </p>
     *
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static class TrieCache {
        /** The root nodes for the trie. */
        private final TrieNode[] myRoots;

        /**
         * Creates a new TrieCache.
         */
        public TrieCache() {
            myRoots = new TrieNode[256];
        }

        /**
         * Adds an entry to the trie cache.
         *
         * @param source
         *            The source of the bytes in the string.
         * @param value
         *            The value to associate with the entry.
         */
        public void addEntry(final byte[] source, final String value) {

            // Minus 1 for the terminal null.
            final int last = source.length - 1;

            final int firstByte = source[0] & 0xFF;
            TrieNode node = myRoots[firstByte];
            if (node == null) {
                node = myRoots[firstByte] = new TrieNode(source[0]);
            }

            // Walk the Trie to the end node.
            for (int i = 1; i < last; ++i) {
                TrieNode child = node.child(source[i]);

                if (child == null) {
                    child = new TrieNode(source[i]);
                    node.addChild(child);
                }

                node = child;
            }

            node.setDecoded(value);

        }

        /**
         * Finds the value for the byte string, if known to the cache. Returns
         * null on a cache miss.
         *
         * @param source
         *            The source of the bytes in the string.
         * @param offset
         *            The offset of the first byte to decode.
         * @param length
         *            The length of the string to decode with a terminal zero
         *            byte.
         * @return The cached decoded string or null in the case of a cache
         *         miss.
         */
        public String find(final byte[] source, final int offset,
                final int length) {

            // Minus 1 for the terminal null.
            final int last = offset + (length - 1);

            final int firstByte = source[offset] & 0xFF;
            TrieNode node = myRoots[firstByte];

            // Walk the Trie to the end node.
            for (int i = offset + 1; (node != null) && (i < last); ++i) {
                node = node.child(source[i]);
            }

            return (node != null) ? node.getDecoded() : null;
        }

        /**
         * Node provides a single node in the trie.
         * <p>
         * {@link #myChildren} is a two dimension array for the children of this
         * node. We use a 2 dimension array to reduce the memory footprint of
         * the trie by taking advantage of the clustering within the encoding of
         * languages. The first dimension of the array we refer to as the
         * {@code zone} and represents the first/high nibble of the value for
         * the child node. The second dimension of the array is the second/low
         * nibble.
         * </p>
         *
         * @api.no This class is <b>NOT</b> part of the drivers API. This class
         *         may be mutated in incompatible ways between any two releases
         *         of the driver.
         * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
         */
        protected static class TrieNode {
            /**
             * The children of the node. See the class javadoc for a description
             * of the structure.
             */
            private TrieNode[][] myChildren;

            /** The decoded value for the node. May be <code>null</code>. */
            private String myDecoded;

            /** The value for the node. */
            private final byte myValue;

            /**
             * Creates a new Node.
             *
             * @param value
             *            The value for the node.
             */
            public TrieNode(final byte value) {
                myValue = value;
                myDecoded = null;

                // No children, yet.
                myChildren = null;
            }

            /**
             * Adds a child node to this node.
             *
             * @param child
             *            The child node to add.
             */
            public void addChild(final TrieNode child) {
                final int value = child.getValue();
                final int zone = (value & 0xF0) >> 4;
                final int index = (value & 0x0F);

                if (myChildren == null) {
                    myChildren = new TrieNode[16][];
                    myChildren[zone] = new TrieNode[16];
                }
                else if (myChildren[zone] == null) {
                    myChildren[zone] = new TrieNode[16];
                }

                myChildren[zone][index] = child;
            }

            /**
             * Returns the child node with the specified value.
             *
             * @param value
             *            The value for the child to find.
             * @return The child node for the value or null if there is node
             *         child with that value.
             */
            public TrieNode child(final byte value) {
                final int zone = (value & 0xF0) >> 4;
                final int index = (value & 0x0F);

                if ((myChildren != null) && (myChildren[zone] != null)) {
                    return myChildren[zone][index];
                }
                return null;
            }

            /**
             * Returns the node's decoded value.
             *
             * @return The node's decoded value.
             */
            public String getDecoded() {
                return myDecoded;
            }

            /**
             * Returns the node's value.
             *
             * @return The node's value.
             */
            public byte getValue() {
                return myValue;
            }

            /**
             * Sets the decoded value for the node.
             *
             * @param decoded
             *            The decoded value for the node.
             */
            public void setDecoded(final String decoded) {
                myDecoded = decoded;
            }
        }
    }

}
