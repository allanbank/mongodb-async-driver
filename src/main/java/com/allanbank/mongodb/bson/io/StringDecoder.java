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

/**
 * StringDecoder provides a decoder for byte arrays into strings that uses a
 * trie data structure to cache recurring strings.
 * <p>
 * There are two controls on the amount of caching instances of this class will
 * do:
 * <ol>
 * <li>
 * {@link #getMaxCacheLength()} limits the length of the encoded bytes this
 * class will try and retrieve from the cache. Setting this value to zero
 * disables the cache and limit any memory overhead.</li>
 * <li>{@link #getMaxCacheEntries()} controls the number of cached strings in
 * the trie. Each entry represents a single encoded strings so if you wanted to
 * cache 100 strings of length 10 bytes each then you would set the value to 100
 * but there may be up to 1000 nodes in the trie.</li>
 * </ol>
 * </p>
 * <p>
 * This class is <b>not</b> thread safe.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StringDecoder {

    /** The default maximum number of entries to keep in the trie cache. */
    public static final int DEFAULT_MAX_CACHE_ENTRIES = StringEncoder.DEFAULT_MAX_CACHE_ENTRIES;

    /** The default maximum length byte array to cache. */
    public static final int DEFAULT_MAX_CACHE_LENGTH = StringEncoder.DEFAULT_MAX_CACHE_LENGTH;

    /** UTF-8 Character set for encoding strings. */
    /* package */final static Charset UTF8 = StringEncoder.UTF8;

    /** The byte value limit for a ASCII character. */
    private static final int ASCII_LIMIT = StringEncoder.ASCII_LIMIT;

    /** A builder for the ASCII strings. */
    private final StringBuilder myBuilder = new StringBuilder(64);

    /** The number of cached strings in the trie. */
    private int myCachedEntryCount;

    /**
     * The head of a linked list through the nodes of the Trie. The head is the
     * most recently accessed node.
     */
    private final Node myHead;

    /**
     * The maximum depth of the nodes in the trie. This can be used to stop a
     * single long string from pushing useful values out of the cache.
     */
    private int myMaxCacheLength;

    /** The maximum number of entries to have in the trie. */
    private int myMaxCachEntries;

    /** The root nodes for the trie. */
    private Node[] myRoots;

    /**
     * The tail of a linked list through the nodes of the Trie. The tail is the
     * least recently accessed node.
     */
    private final Node myTail;

    /**
     * Creates a new TrieStringCache.
     */
    public StringDecoder() {
        super();

        myRoots = null;
        myHead = new Node(null, (byte) 0);
        myTail = new Node(null, (byte) 0);

        myTail.addAfter(myHead);

        myCachedEntryCount = 0;
        myMaxCacheLength = DEFAULT_MAX_CACHE_LENGTH;
        myMaxCachEntries = DEFAULT_MAX_CACHE_ENTRIES;
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

        // Check for the easiest value to cache.
        if (length <= 1) {
            return "";
        }
        // And for values blowing out the cache.
        else if (myMaxCacheLength < length) {
            return fastDecode(source, offset, length - 1);
        }
        // Check for the terminal null.
        else if (source[(offset + length) - 1] != 0) {
            throw new StreamCorruptedException(
                    "Expected a null character at the end of the string.");
        }

        final int first = source[offset] & 0xFF;
        Node node = findRoot(first);

        // Walk the Trie to the end node.
        final int last = length - 1; // For the terminal null.
        for (int i = 1; i < last; ++i) {
            final int read = source[offset + i] & 0xFF;
            Node child = node.child((byte) read);
            if (child == null) {
                child = createNode(node, read);
            }
            else {
                touch(child);
            }
            node = child;
        }

        return finish(source, offset, last, node);
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
     * Returns the maximum depth of the nodes in the trie. This can be used to
     * stop a single long string from pushing useful values out of the cache.
     * 
     * @return The maximum depth of the nodes in the trie.
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
     * Sets the value of maximum depth of the nodes in the trie to the new
     * value. This can be used to stop a single long string from pushing useful
     * values out of the cache.
     * 
     * @param maxlength
     *            The new value for the maximum depth of the nodes in the trie.
     */
    public void setMaxCacheLength(final int maxlength) {
        myMaxCacheLength = maxlength;

        // The user has turned the cache off. Release all of the memory.
        if (maxlength <= 0) {
            myRoots = null;

            // Reset the list.
            myHead.removeFromList();
            myTail.addAfter(myHead);

            myCachedEntryCount = 0;
        }
    }

    /**
     * Adds a node to the head of the recent linked list.
     * 
     * @param node
     *            The node to add to the recent list.
     */
    private void addToList(final Node node) {
        node.addAfter(myHead);
    }

    /**
     * Creates a new node in the trie with the specified parent and value. This
     * method may remove nodes from the trie if it has grown beyond the
     * {@link #setMaxCacheEntries maximum cached entries}.
     * 
     * @param parent
     *            The parent of the node in the trie.
     * @param value
     *            the value for the node.
     * @return The new node.
     */
    private Node createNode(final Node parent, final int value) {

        final Node node = new Node(parent, (byte) value);
        addToList(node);

        if (parent != null) {
            parent.addChild(node);
        }

        return node;
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
            if (b < ASCII_LIMIT) {
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

    /**
     * Finds the root node for the value.
     * 
     * @param first
     *            The first value in the trie.
     * @return The root node for the trie.
     */
    private Node findRoot(final int first) {

        Node node;
        if (myRoots == null) {
            myRoots = new Node[256];
            node = createNode(null, first);
            myRoots[first] = node;
        }
        else {
            node = myRoots[first];
            if (node == null) {
                node = createNode(null, first);
                myRoots[first] = node;
            }
            else {
                touch(node);
            }
        }

        return node;
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
     * @param node
     *            The last node for the string in the Trie.
     * @return The value for the string.
     */
    private String finish(final byte[] source, final int offset,
            final int length, final Node node) {
        if (node.getDecoded() != null) {
            // Cache hit.
            return node.getDecoded();
        }

        while (myMaxCachEntries < (myCachedEntryCount + 1)) {
            // remove the tail and any children.
            myCachedEntryCount -= myTail.getPrevious().remove();
        }

        final String result = fastDecode(source, offset, length);
        node.setDecoded(result);
        myCachedEntryCount += 1;

        return result;
    }

    /**
     * Moves the node to the head of the recent list.
     * 
     * @param node
     *            The node to move to the head of the recent list.
     */
    private void touch(final Node node) {
        node.removeFromList();
        addToList(node);
    }

    /**
     * Node provides a single node in the trie.
     * <p>
     * Each node holds its value and several pointers:
     * <ul>
     * <li>The {@link #myNext} and {@link #myPrevious} values are the pointers
     * to the next and previous elements in the recent linked list.</li>
     * <li>The {@link #myParent} is the parent of this node. For the root nodes
     * this is null.</li>
     * <li> {@link #myChildren} is a two dimension array for the children of this
     * node. We use a 2 dimension array to reduce the memory footprint of the
     * trie by taking advantage of the clustering within the encoding of
     * languages. The first dimension of the array we refer to as the
     * {@code zone} and represents the first/high nibble of the value for the
     * child node. The second dimension of the array is the second/low nibble.</li>
     * </ul>
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static class Node {
        /**
         * The children of the node. See the class javadoc for a description of
         * the structure.
         */
        private Node[][] myChildren;

        /** The decoded value for the node. May be <code>null</code>. */
        private String myDecoded;

        /** The next node to this node in the recent linked list. */
        private Node myNext;

        /** The parent node. May be <code>null</code>. */
        private final Node myParent;

        /** The previous node to this node in the recent linked list. */
        private Node myPrevious;

        /** The value for the node. */
        private final byte myValue;

        /**
         * Creates a new Node.
         * 
         * @param parent
         *            The parent node. May be <code>null</code>.
         * @param value
         *            The value for the node.
         */
        public Node(final Node parent, final byte value) {
            myParent = parent;
            myValue = value;

            myDecoded = null;

            // No children, yet.
            myChildren = null;

            // Not on the list, yet.
            myPrevious = null;
            myNext = null;
        }

        /**
         * Add a node after the provided node.
         * 
         * @param node
         *            The head node.
         */
        public void addAfter(final Node node) {
            myPrevious = node;
            myNext = node.myNext;

            if (node.myNext != null) {
                node.myNext.myPrevious = this;
            }
            node.myNext = this;
        }

        /**
         * Adds a child node to this node.
         * 
         * @param child
         *            The child node to add.
         */
        public void addChild(final Node child) {
            final int value = child.getValue();
            final int zone = (value & 0xF0) >> 4;
            final int index = (value & 0x0F);

            if (myChildren == null) {
                myChildren = new Node[16][];
                myChildren[zone] = new Node[16];
            }
            else if (myChildren[zone] == null) {
                myChildren[zone] = new Node[16];
            }

            myChildren[zone][index] = child;
        }

        /**
         * Returns the child node with the specified value.
         * 
         * @param value
         *            The value for the child to find.
         * @return The child node for the value or null if there is node child
         *         with that value.
         */
        public Node child(final byte value) {
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
         * Returns the next node in the recent list.
         * 
         * @return The next node in the recent list.
         */
        public Node getNext() {
            return myNext;
        }

        /**
         * Returns the previous node in the recent list.
         * 
         * @return The previous node in the recent list.
         */
        public Node getPrevious() {
            return myPrevious;
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
         * Removes the node and its children from the trie.
         * 
         * @return The number of nodes removed.
         */
        public int remove() {
            removeFromList();
            if (myParent != null) {
                myParent.removeChild(this);
            }

            // Remove the children too.
            int removed = (myDecoded != null) ? 1 : 0;
            if (myChildren != null) {
                for (final Node[] zone : myChildren) {
                    if (zone != null) {
                        for (final Node child : zone) {
                            if (child != null) {
                                removed += child.remove();
                            }
                        }
                    }
                }
            }

            return removed;
        }

        /**
         * Removes the node from the recent linked list.
         */
        public void removeFromList() {
            if (myNext != null) {
                myNext.myPrevious = myPrevious;
            }
            if (myPrevious != null) {
                myPrevious.myNext = myNext;
            }

            myPrevious = myNext = null;
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

        /**
         * Removes the child node from this parent node.
         * 
         * @param child
         *            The child node to remove.
         */
        private void removeChild(final Node child) {
            final int value = child.getValue();
            final int zone = (value & 0xF0) >> 4;
            final int index = (value & 0x0F);

            if ((myChildren != null) && (myChildren[zone] != null)) {
                myChildren[zone][index] = null;
            }
        }
    }
}
