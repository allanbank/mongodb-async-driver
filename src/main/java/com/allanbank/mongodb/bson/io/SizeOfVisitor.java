/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.nio.charset.Charset;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * A visitor to determine the size of the documents it visits. Intermediate
 * document sizes are cached for faster access later.
 * <p>
 * Caching is accomplished via a simple singly linked list of the cached
 * documents. This works since the document will be written in the same in-order
 * traversal of the document tree that this visitor follows. As each level of
 * the tree is written this visitor should have {@link #rewind()} called to set
 * the size back to zero and also remove the head from the document cache list.
 * If the next document written is the expected in-order traversal there is a
 * cache hit and the size is simply read from the cache's head node. Rinse,
 * repeat.
 * </p>
 * <p>
 * A custom list {@link CachedSizeNode node type} is used instead of a generic
 * linked list to remove the overhead of extra object allocations for the node
 * and the value.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SizeOfVisitor implements Visitor {
    /** UTF-8 Character set for encoding strings. */
    public final static Charset UTF8 = Charset.forName("UTF-8");

    /** The head of the list of cached sizes. */
    private CachedSizeNode myHead = null;

    /** The computed size. */
    private int mySize;

    /** The tail of the list of cached sizes. */
    private CachedSizeNode myTail = null;

    /**
     * Creates a new SizeOfVisitor.
     */
    public SizeOfVisitor() {
        super();
        mySize = 0;
    }

    /**
     * Returns the visitor's output buffer.
     * 
     * @param string
     *            The 'C' string to determine the size of.
     * @return The visitor's output buffer.
     */
    public int computeCStringSize(final String string) {
        return utf8Size(string) + 1;
    }

    /**
     * Returns the visitor's output buffer.
     * 
     * @param string
     *            The 'UTF8' string to determine the size of.
     * @return The visitor's output buffer.
     */
    public int computeStringSize(final String string) {
        return 4 + utf8Size(string) + 1;
    }

    /**
     * Return the current Size of the written document.
     * 
     * @return The current size of the encoded document.
     */
    public int getSize() {
        return mySize;
    }

    /**
     * Resets the size to zero and clears the cached documents. Use
     * {@link #rewind()} to just set the size to zero and not clear the cached
     * documents.
     */
    public void reset() {
        mySize = 0;
        myHead = myTail = null;
    }

    /**
     * Resets the size to zero but does not clear the cached documents. Use
     * {@link #reset()} to set the size to zero and clear the cached documents.
     */
    public void rewind() {
        mySize = 0;
        if (myHead != null) {
            myHead = myHead.myNext;
        }
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
    public int utf8Size(final String string) {

        int length = 0;
        final int strLength = string.length();
        for (int i = 0; i < strLength; ++i) {
            final int c = string.charAt(i);
            if (c < 0x0080) {
                length += 1;
            }
            else if (c < 0x0800) {
                length += 2;
            }
            else {
                // To complicated above here, surrogates and what not.
                return length + string.substring(i).getBytes(UTF8).length;
            }
            // Below does not work with invalid surrogate sequences.
            // Above is the same logic used to encode the string so is "safe".
            // Need to figure out if it is "good enough".
            /**
             * <pre>
             *             else if (c < 0x1000) {
             *                 length += 3;
             *             }
             *             else {
             *                 // Have to worry about surrogate pairs.
             *                 if (Character.isHighSurrogate((char) c)
             *                         && ((i + 1) < strLength)
             *                         && Character.isLowSurrogate(string.charAt(i + 1))) {
             *                     // Consume the second character too.
             *                     i += 1;
             *                     c = Character.toCodePoint((char) c, string.charAt(i));
             *                 }
             * 
             *                 if (c <= 0xFFFF) {
             *                     length += 3;
             *                 }
             *                 else {
             *                     length += 4;
             *                 }
             *             }
             * </pre>
             */
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final List<Element> elements) {

        if ((myHead != null) && (myHead.myElements == elements)) {
            mySize += myHead.mySize;
        }
        else {
            // int - 4
            // elements...
            // byte

            final CachedSizeNode mine = new CachedSizeNode(elements);
            if (myHead == null) {
                myHead = myTail = mine;
            }
            else {
                myTail.setNext(mine);
                myTail = mine;
            }

            final int beforeSize = mySize;
            mySize += 4;
            for (final Element element : elements) {
                // Optimization to avoid the array copy.
                if (element.getType() == ElementType.BINARY) {
                    BinaryElement be = (BinaryElement) element;
                    doVisitBinary(be.getName(), be.getSubType(), be.length());
                }
                else {
                    element.accept(this);
                }
            }
            mySize += 1;

            mine.setSize(mySize - beforeSize);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitArray(final String name, final List<Element> elements) {

        mySize += 1;
        mySize += computeCStringSize(name);
        visit(elements);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitBinary(final String name, final byte subType,
            final byte[] data) {

        final int dataLength = data.length;

        doVisitBinary(name, subType, dataLength);
    }

    /**
     * Computes the size of the binary based on the name, type and length of the
     * data.
     * 
     * @param name
     *            The name of the element.
     * @param subType
     *            The sub-type of the binary element.
     * @param dataLength
     *            The length of data contained in the element.
     */
    private void doVisitBinary(final String name, final byte subType,
            final int dataLength) {
        mySize += 1;
        mySize += computeCStringSize(name);

        switch (subType) {
        case 2: {
            mySize += (4 + 1 + 4 + dataLength);
            break;

        }
        case 0:
        default:
            mySize += (4 + 1 + dataLength);
            break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitBoolean(final String name, final boolean value) {

        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitDBPointer(final String name, final String databaseName,
            final String collectionName, final ObjectId id) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += computeStringSize(databaseName + "." + collectionName);
        mySize += (4 + 8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitDocument(final String name, final List<Element> elements) {
        mySize += 1;
        mySize += computeCStringSize(name);
        visit(elements);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitDouble(final String name, final double value) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 8;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitInteger(final String name, final int value) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 4;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitJavaScript(final String name, final String code) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += computeStringSize(code);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitJavaScript(final String name, final String code,
            final Document scope) {
        mySize += 1;
        mySize += computeCStringSize(name);

        mySize += 4;
        mySize += computeStringSize(code);

        scope.accept(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitLong(final String name, final long value) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 8;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitMaxKey(final String name) {
        mySize += 1;
        mySize += computeCStringSize(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitMinKey(final String name) {
        mySize += 1;
        mySize += computeCStringSize(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitMongoTimestamp(final String name, final long value) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 8;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitNull(final String name) {
        mySize += 1;
        mySize += computeCStringSize(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitObjectId(final String name, final ObjectId id) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += (4 + 8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitRegularExpression(final String name, final String pattern,
            final String options) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += computeCStringSize(pattern);
        mySize += computeCStringSize(options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitString(final String name, final String value) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += computeStringSize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitSymbol(final String name, final String symbol) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += computeStringSize(symbol);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitTimestamp(final String name, final long timestamp) {
        mySize += 1;
        mySize += computeCStringSize(name);
        mySize += 8;
    }

    /**
     * CachedSizeNode provides a node in a singly linked list that forms the
     * cache for the sizes of lists of elements.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class CachedSizeNode {

        /** The elements we are caching the size of. */
        /* package */List<Element> myElements;

        /** The next node in the singly linked list. */
        /* package */CachedSizeNode myNext;

        /** The cached size. */
        /* package */int mySize;

        /**
         * Creates a new CachedSizeNode.
         * 
         * @param elements
         *            The elements to cache the size of.
         */
        public CachedSizeNode(final List<Element> elements) {
            myElements = elements;
            mySize = 0;
            myNext = null;
        }

        /**
         * Returns the elements value.
         * 
         * @return The elements value.
         */
        public List<Element> getElements() {
            return myElements;
        }

        /**
         * Returns the next value.
         * 
         * @return The next value.
         */
        public CachedSizeNode getNext() {
            return myNext;
        }

        /**
         * Returns the size value.
         * 
         * @return The size value.
         */
        public int getSize() {
            return mySize;
        }

        /**
         * Sets the value of next to the new value.
         * 
         * @param next
         *            The new value for the next.
         */
        public void setNext(final CachedSizeNode next) {
            myNext = next;
        }

        /**
         * Sets the value of size to the new value.
         * 
         * @param size
         *            The new value for the size.
         */
        public void setSize(final int size) {
            mySize = size;
        }
    }
}
