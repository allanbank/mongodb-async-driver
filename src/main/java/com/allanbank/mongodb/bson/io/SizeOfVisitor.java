/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * A visitor to determine the size of the documents it visits. Intermediate
 * document sizes are cached for faster access later.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class SizeOfVisitor implements Visitor {

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
        int length = 0;
        final int strLength = string.length();
        for (int i = 0; i < strLength; ++i) {
            int c = string.charAt(i);
            if (c < 0x0080) {
                length += 1;
            }
            else if (c < 0x0800) {
                length += 2;
            }
            else if (c < 0x1000) {
                length += 3;
            }
            else {
                // Have to worry about surrogate pairs.
                if (Character.isHighSurrogate((char) c)
                        && ((i + 1) < strLength)
                        && Character.isLowSurrogate(string.charAt(i + 1))) {
                    // Consume the second character too.
                    i += 1;
                    c = Character.toCodePoint((char) c, string.charAt(i));
                }

                if (c <= 0xFFFF) {
                    length += 3;
                }
                else {
                    length += 4;
                }
            }
        }

        return length;
    }

    /** The computed size. */
    private int mySize;

    /**
     * Creates a new SizeOfVisitor.
     */
    public SizeOfVisitor() {
        super();
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
    }

    /**
     * Resets the size to zero but does not clear the cached documents. Use
     * {@link #reset()} to set the size to zero and clear the cached documents.
     */
    public void rewind() {
        mySize = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final List<Element> elements) {
        // int - 4
        // elements...
        // byte

        mySize += 4;
        for (final Element element : elements) {
            element.accept(this);
        }
        mySize += 1;
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

        mySize += 1;
        mySize += computeCStringSize(name);

        switch (subType) {
        case 2: {
            mySize += (4 + 1 + 4 + data.length);
            break;

        }
        case 0:
        default:
            mySize += (4 + 1 + data.length);
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
}