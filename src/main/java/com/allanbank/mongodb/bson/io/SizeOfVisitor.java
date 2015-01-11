/*
 * #%L
 * SizeOfVisitor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.nio.charset.Charset;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * A visitor to determine the size of the documents it visits.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public class SizeOfVisitor
        implements Visitor {
    /** UTF-8 Character set for encoding strings. */
    public final static Charset UTF8 = StringDecoder.UTF8;

    /** The computed size. */
    private int mySize;

    /** The encoder for strings. */
    private final StringEncoder myStringEncoder;

    /**
     * Creates a new SizeOfVisitor.
     */
    public SizeOfVisitor() {
        this(null);
    }

    /**
     * Creates a new SizeOfVisitor.
     *
     * @param encoder
     *            The encoder for strings.
     */
    public SizeOfVisitor(final StringEncoder encoder) {
        super();
        mySize = 0;
        myStringEncoder = encoder;
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
        if (myStringEncoder != null) {
            return myStringEncoder.encodeSize(string);
        }
        return StringEncoder.utf8Size(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final List<Element> elements) {
        mySize += 4;
        for (final Element element : elements) {
            // Optimization to avoid the array copy.
            if (element.getType() == ElementType.BINARY) {
                final BinaryElement be = (BinaryElement) element;
                doVisitBinary(be.getName(), be.getSubType(), be.length());
            }
            else {
                element.accept(this);
            }
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

        final int dataLength = data.length;

        doVisitBinary(name, subType, dataLength);
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
}
