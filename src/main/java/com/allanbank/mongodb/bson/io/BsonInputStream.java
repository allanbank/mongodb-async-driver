/*
 * #%L
 * BsonInputStream.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.JavaScriptElement;
import com.allanbank.mongodb.bson.element.JavaScriptWithScopeElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MaxKeyElement;
import com.allanbank.mongodb.bson.element.MinKeyElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * {@link BsonInputStream} provides a class to read BSON documents based on the
 * <a href="http://bsonspec.org/">BSON specification</a>.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonInputStream extends InputStream {

    /** UTF-8 Character set for encoding strings. */
    public final static Charset UTF8 = StringDecoder.UTF8;

    /** The buffered data. */
    private byte[] myBuffer;

    /** The offset into the current buffer. */
    private int myBufferLimit;

    /** The offset into the current buffer. */
    private int myBufferOffset;

    /** Tracks the number of bytes that have been read by the stream. */
    private long myBytesRead;

    /** The underlying input stream. */
    private final InputStream myInput;

    /** The decoder for strings. */
    private final StringDecoder myStringDecoder = new StringDecoder();

    /**
     * Creates a BSON document reader.
     * 
     * @param input
     *            the underlying stream to read from.
     */
    public BsonInputStream(final InputStream input) {
        this(input, 8 * 1024); // 8K to start.
    }

    /**
     * Creates a BSON document reader.
     * 
     * @param input
     *            the underlying stream to read from.
     * @param expectedMaxDocumentSize
     *            The expected maximum size for a document. If this guess is
     *            wrong then there may be incremental allocations of the read
     *            buffer.
     */
    public BsonInputStream(final InputStream input,
            final int expectedMaxDocumentSize) {
        myInput = input;
        myBuffer = new byte[expectedMaxDocumentSize];
        myBufferOffset = 0;
        myBufferLimit = 0;
        myBytesRead = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the number of bytes in the buffer and from the
     * source stream.
     * </p>
     */
    @Override
    public int available() throws IOException {
        return availableInBuffer() + myInput.available();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the wrapped {@link InputStream}.
     * </p>
     */
    @Override
    public void close() throws IOException {
        myInput.close();
    }

    /**
     * Returns the number of bytes that have been read by the stream.
     * 
     * @return The number of bytes that have been read from the stream.
     */
    public long getBytesRead() {
        return myBytesRead + myBufferOffset;
    }

    /**
     * Returns the maximum number of strings that may have their encoded form
     * cached.
     * 
     * @return The maximum number of strings that may have their encoded form
     *         cached.
     */
    public int getMaxCachedStringEntries() {
        return myStringDecoder.getMaxCacheEntries();
    }

    /**
     * Returns the maximum length for a string that the stream is allowed to
     * cache.
     * 
     * @return The maximum length for a string that the stream is allowed to
     *         cache.
     */
    public int getMaxCachedStringLength() {
        return myStringDecoder.getMaxCacheLength();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw an {@link UnsupportedOperationException}.
     * </p>
     */
    @Override
    public synchronized void mark(final int readlimit) {
        throw new UnsupportedOperationException("Mark not supported.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return false.
     * </p>
     */
    @Override
    public boolean markSupported() {
        return false;
    }

    /**
     * Tries to prefetch the requested number of bytes from the underlying
     * stream.
     * 
     * @param size
     *            The number of bytes to try and read.
     * @throws IOException
     *             On a failure to read from the underlying stream.
     */
    public final void prefetch(final int size) throws IOException {
        fetch(size, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to track the bytes that have been read.
     * </p>
     */
    @Override
    public int read() throws IOException {
        if (ensureFetched(1) != 1) {
            return -1; // EOF.
        }

        final int read = (myBuffer[myBufferOffset] & 0xFF);

        myBufferOffset += 1;

        return read;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to track the bytes that have been read.
     * </p>
     */
    @Override
    public int read(final byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to track the bytes that have been read.
     * </p>
     */
    @Override
    public int read(final byte b[], final int off, final int len)
            throws IOException {
        final int read = ensureFetched(len - off);

        System.arraycopy(myBuffer, myBufferOffset, b, off, read);

        myBufferOffset += read;

        return read;
    }

    /**
     * Reads a "cstring" value from the stream:<code>
     * <pre>
     * cstring 	::= 	(byte*) "\x00"
     * </pre>
     * </code>
     * <p>
     * <blockquote> CString - Zero or more modified UTF-8 encoded characters
     * followed by '\x00'. The (byte*) MUST NOT contain '\x00', hence it is not
     * full UTF-8. </blockquote>
     * </p>
     * 
     * @return The string value.
     * @throws EOFException
     *             On insufficient data for the integer.
     * @throws IOException
     *             On a failure reading the integer.
     */
    public String readCString() throws EOFException, IOException {

        while (true) {
            for (int i = myBufferOffset; i < myBufferLimit; ++i) {
                final byte b = myBuffer[i];
                if (b == 0) {
                    // Found the end.
                    final int offset = myBufferOffset;
                    final int length = (1 + i) - offset;

                    // Advance the buffer.
                    myBufferOffset = i + 1;

                    return myStringDecoder.decode(myBuffer, offset, length);
                }
            }

            // Need more data.
            ensureFetched(availableInBuffer() + 1);
        }
    }

    /**
     * Reads a BSON document element: <code>
     * <pre>
     * document 	::= 	int32 e_list "\x00"
     * </pre>
     * </code>
     * 
     * @return The Document.
     * @throws EOFException
     *             On insufficient data for the document.
     * @throws IOException
     *             On a failure reading the document.
     */
    public Document readDocument() throws IOException {

        // The total length of the document.
        final int size = readInt();

        prefetch(size - 4);

        return new RootDocument(readElements(), false, size);
    }

    /**
     * Reads the complete set of bytes from the stream or throws an
     * {@link EOFException}.
     * 
     * @param buffer
     *            The buffer into which the data is read.
     * @exception EOFException
     *                If the input stream reaches the end before reading all the
     *                bytes.
     * @exception IOException
     *                On an error reading from the underlying stream.
     */
    public void readFully(final byte[] buffer) throws EOFException, IOException {
        readFully(buffer, 0, buffer.length);
    }

    /**
     * Reads a little-endian 4 byte signed integer from the stream.
     * 
     * @return The integer value.
     * @throws EOFException
     *             On insufficient data for the integer.
     * @throws IOException
     *             On a failure reading the integer.
     */
    public int readInt() throws EOFException, IOException {
        if (ensureFetched(4) != 4) {
            throw new EOFException();
        }

        // Little endian.
        int result = (myBuffer[myBufferOffset] & 0xFF);
        result += (myBuffer[myBufferOffset + 1] & 0xFF) << 8;
        result += (myBuffer[myBufferOffset + 2] & 0xFF) << 16;
        result += (myBuffer[myBufferOffset + 3] & 0xFF) << 24;

        myBufferOffset += 4;

        return result;
    }

    /**
     * Reads a little-endian 8 byte signed integer from the stream.
     * 
     * @return The long value.
     * @throws EOFException
     *             On insufficient data for the long.
     * @throws IOException
     *             On a failure reading the long.
     */
    public long readLong() throws EOFException, IOException {
        if (ensureFetched(8) != 8) {
            throw new EOFException();
        }

        // Little endian.
        long result = (myBuffer[myBufferOffset] & 0xFFL);
        result += (myBuffer[myBufferOffset + 1] & 0xFFL) << 8;
        result += (myBuffer[myBufferOffset + 2] & 0xFFL) << 16;
        result += (myBuffer[myBufferOffset + 3] & 0xFFL) << 24;
        result += (myBuffer[myBufferOffset + 4] & 0xFFL) << 32;
        result += (myBuffer[myBufferOffset + 5] & 0xFFL) << 40;
        result += (myBuffer[myBufferOffset + 6] & 0xFFL) << 48;
        result += (myBuffer[myBufferOffset + 7] & 0xFFL) << 56;

        myBufferOffset += 8;

        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw an {@link UnsupportedOperationException}.
     * </p>
     */
    @Override
    public synchronized void reset() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Mark not supported.");
    }

    /**
     * Sets the value of maximum number of strings that may have their encoded
     * form cached.
     * 
     * @param maxCacheEntries
     *            The new value for the maximum number of strings that may have
     *            their encoded form cached.
     */
    public void setMaxCachedStringEntries(final int maxCacheEntries) {
        myStringDecoder.setMaxCacheEntries(maxCacheEntries);
    }

    /**
     * Sets the value of length for a string that the stream is allowed to cache
     * to the new value. This can be used to stop a single long string from
     * pushing useful values out of the cache.
     * 
     * @param maxlength
     *            The new value for the length for a string that the encoder is
     *            allowed to cache.
     */
    public void setMaxCachedStringLength(final int maxlength) {
        myStringDecoder.setMaxCacheLength(maxlength);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to track the bytes that have been skipped.
     * </p>
     */
    @Override
    public long skip(final long n) throws IOException {

        long skipped = Math.min(n, availableInBuffer());
        myBufferOffset += skipped;

        if (skipped < n) {
            // Exhausted the buffer - skip in the source.
            final long streamSkipped = myInput.skip(n - skipped);
            myBytesRead += streamSkipped;
            skipped += streamSkipped;
        }

        return skipped;
    }

    /**
     * Returns the number of bytes available in the buffer.
     * 
     * @return The number of bytes available in the buffer.
     */
    protected final int availableInBuffer() {
        return myBufferLimit - myBufferOffset;
    }

    /**
     * Reads a BSON Array element: <code>
     * <pre>
     * "\x04" e_name document
     * </pre>
     * </code>
     * 
     * @return The {@link ArrayElement}.
     * @throws EOFException
     *             On insufficient data for the document.
     * @throws IOException
     *             On a failure reading the document.
     */
    protected ArrayElement readArrayElement() throws IOException {

        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final int fetch = readInt(); // The total length of the array elements.
        prefetch(fetch - 4);
        final List<Element> elements = readElements();
        final long size = getBytesRead() - start;

        return new ArrayElement(name, elements, size);
    }

    /**
     * Reads a {@link BinaryElement}'s contents: <code><pre>
     * binary 	::= 	int32 subtype (byte*)
     * subtype 	::= 	"\x00" 	Binary / Generic
     * 	           | 	"\x01" 	Function
     * 	           | 	"\x02" 	Binary (Old)
     * 	           | 	"\x03" 	UUID
     * 	           | 	"\x05" 	MD5
     * 	           | 	"\x80" 	User defined
     * </pre>
     * </code>
     * 
     * @return The {@link BinaryElement}.
     * @throws IOException
     *             On a failure reading the binary data.
     */
    protected BinaryElement readBinaryElement() throws IOException {

        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        int length = readInt();

        final int subType = read();
        if (subType < 0) {
            throw new EOFException();
        }

        // Old binary handling.
        if (subType == 2) {
            final int anotherLength = readInt();

            assert (anotherLength == (length - 4)) : "Binary Element Subtye 2 "
                    + "length should be outer length - 4.";

            length -= 4;
        }
        else if ((subType == UuidElement.LEGACY_UUID_SUBTTYPE)
                || (subType == UuidElement.UUID_SUBTTYPE)) {

            final byte[] binary = new byte[length];
            readFully(binary);

            final long size = getBytesRead() - start;
            try {
                return new UuidElement(name, (byte) subType, binary, size);
            }
            catch (final IllegalArgumentException iae) {
                // Just use the vanilla BinaryElement.
                return new BinaryElement(name, (byte) subType, binary, size);
            }
        }

        final long size = getBytesRead() - start;
        return new BinaryElement(name, (byte) subType, this, length, size
                + length);
    }

    /**
     * Reads a {@link BooleanElement} from the stream.
     * 
     * @return The {@link BooleanElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link BooleanElement}.
     */
    protected BooleanElement readBooleanElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final boolean value = (read() == 1);

        final long size = getBytesRead() - start;

        return new BooleanElement(name, value, size);
    }

    /**
     * Reads a {@code DBPointerElement} from the stream.
     * 
     * @return The {@code DBPointerElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@code DBPointerElement}.
     * @deprecated Per the BSON specification.
     */
    @Deprecated
    protected Element readDBPointerElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final String dbDotCollection = readString();
        final int timestamp = EndianUtils.swap(readInt());
        final long machineId = EndianUtils.swap(readLong());

        final long size = getBytesRead() - start;

        String db = dbDotCollection;
        String collection = "";
        final int firstDot = dbDotCollection.indexOf('.');
        if (0 <= firstDot) {
            db = dbDotCollection.substring(0, firstDot);
            collection = dbDotCollection.substring(firstDot + 1);
        }
        return new com.allanbank.mongodb.bson.element.DBPointerElement(name,
                db, collection, new ObjectId(timestamp, machineId), size);
    }

    /**
     * Reads a BSON Subdocument element: <code>
     * <pre>
     * "\x03" e_name document
     * </pre>
     * </code>
     * 
     * @return The {@link ArrayElement}.
     * @throws IOException
     *             On a failure reading the document.
     */
    protected DocumentElement readDocumentElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final int fetch = readInt(); // The total length of the sub-document
        // elements.
        prefetch(fetch - 4);
        final List<Element> elements = readElements();

        final long size = getBytesRead() - start;

        return new DocumentElement(name, elements, true, size);
    }

    /**
     * Reads a {@link DoubleElement} from the stream.
     * 
     * @return The {@link DoubleElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link DoubleElement}.
     */
    protected DoubleElement readDoubleElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final double value = Double.longBitsToDouble(readLong());

        final long size = getBytesRead() - start;

        return new DoubleElement(name, value, size);
    }

    /**
     * Reads the element:<code>
     * <pre>
     * element 	::= 	"\x01" e_name double 			Floating point
     * 	           | 	"\x02" e_name string 			UTF-8 string
     * 	           | 	"\x03" e_name document 			Embedded document
     * 	           | 	"\x04" e_name document 			Array
     * 	           | 	"\x05" e_name binary 			Binary data
     * 	           | 	"\x06" e_name 					Undefined — Deprecated
     * 	           | 	"\x07" e_name (byte*12) 		ObjectId
     * 	           | 	"\x08" e_name "\x00" 			Boolean "false"
     * 	           | 	"\x08" e_name "\x01" 			Boolean "true"
     * 	           | 	"\x09" e_name int64 			UTC datetime
     * 	           | 	"\x0A" e_name 					Null value
     * 	           | 	"\x0B" e_name cstring cstring 	Regular expression
     * 	           | 	"\x0C" e_name string (byte*12) 	DBPointer — Deprecated
     * 	           | 	"\x0D" e_name string 			JavaScript code
     * 	           | 	"\x0E" e_name string 			Symbol
     * 	           | 	"\x0F" e_name code_w_s 			JavaScript code w/ scope
     * 	           | 	"\x10" e_name int32 			32-bit Integer
     * 	           | 	"\x11" e_name int64 			Timestamp
     * 	           | 	"\x12" e_name int64 			64-bit integer
     * 	           | 	"\xFF" e_name 					Min key
     * 	           | 	"\x7F" e_name 					Max key
     * </pre>
     * </code>
     * 
     * @param token
     *            The element's token.
     * @return The Element.
     * @throws EOFException
     *             On insufficient data for the element.
     * @throws IOException
     *             On a failure reading the element.
     */
    @SuppressWarnings("deprecation")
    protected Element readElement(final byte token) throws EOFException,
            IOException {
        final ElementType type = ElementType.valueOf(token);
        if (type == null) {
            throw new StreamCorruptedException("Unknown element type: 0x"
                    + Integer.toHexString(token & 0xFF) + ".");
        }
        switch (type) {
        case ARRAY: {
            return readArrayElement();
        }
        case BINARY: {
            return readBinaryElement();
        }
        case DB_POINTER: {
            return readDBPointerElement();
        }
        case DOCUMENT: {
            return readDocumentElement();
        }
        case DOUBLE: {
            return readDoubleElement();
        }
        case BOOLEAN: {
            return readBooleanElement();
        }
        case INTEGER: {
            return readIntegerElement();
        }
        case JAVA_SCRIPT: {
            return readJavaScriptElement();
        }
        case JAVA_SCRIPT_WITH_SCOPE: {
            return readJavaScriptWithScopeElement();
        }
        case LONG: {
            return readLongElement();
        }
        case MAX_KEY: {
            return readMaxKeyElement();
        }
        case MIN_KEY: {
            return readMinKeyElement();
        }
        case MONGO_TIMESTAMP: {
            return readMongoTimestampElement();
        }
        case NULL: {
            return readNullElement();
        }
        case OBJECT_ID: {
            return readObjectIdElement();
        }
        case REGEX: {
            return readRegularExpressionElement();
        }
        case STRING: {
            return readStringElement();
        }
        case SYMBOL: {
            return readSymbolElement();
        }
        case UTC_TIMESTAMP: {
            return readTimestampElement();
        }
        }

        throw new StreamCorruptedException("Unknown element type: "
                + type.name() + ".");
    }

    /**
     * Reads a BSON element list (e_list): <code>
     * <pre>
     * e_list 	::= 	element e_list | ""
     * </pre>
     * </code>
     * 
     * @return The list of elements.
     * @throws EOFException
     *             On insufficient data for the elements.
     * @throws IOException
     *             On a failure reading the elements.
     */
    protected List<Element> readElements() throws EOFException, IOException {
        final List<Element> elements = new ArrayList<Element>();
        int elementToken = read();
        while (elementToken > 0) {
            elements.add(readElement((byte) elementToken));

            elementToken = read();
        }
        if (elementToken < 0) {
            throw new EOFException();
        }
        return elements;
    }

    /**
     * Reads a {@link IntegerElement} from the stream.
     * 
     * @return The {@link IntegerElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link IntegerElement}.
     */
    protected IntegerElement readIntegerElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final int value = readInt();

        final long size = getBytesRead() - start;

        return new IntegerElement(name, value, size);
    }

    /**
     * Reads a {@link JavaScriptElement} from the stream.
     * 
     * @return The {@link JavaScriptElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link JavaScriptElement}.
     */
    protected JavaScriptElement readJavaScriptElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final String javascript = readString();

        final long size = getBytesRead() - start;

        return new JavaScriptElement(name, javascript, size);
    }

    /**
     * Reads a {@link JavaScriptWithScopeElement} from the stream.
     * 
     * @return The {@link JavaScriptWithScopeElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link JavaScriptWithScopeElement}.
     */
    protected JavaScriptWithScopeElement readJavaScriptWithScopeElement()
            throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        readInt(); // Total length - not used.
        final String javascript = readString();
        final Document scope = readDocument();

        final long size = getBytesRead() - start;

        return new JavaScriptWithScopeElement(name, javascript, scope, size);
    }

    /**
     * Reads a {@link LongElement} from the stream.
     * 
     * @return The {@link LongElement}.
     * @throws IOException
     *             On a failure to read the contents of the {@link LongElement}.
     */
    protected LongElement readLongElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final long value = readLong();

        final long size = getBytesRead() - start;

        return new LongElement(name, value, size);
    }

    /**
     * Reads a {@link MaxKeyElement} from the stream.
     * 
     * @return The {@link MaxKeyElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link MaxKeyElement}.
     */
    protected MaxKeyElement readMaxKeyElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();

        final long size = getBytesRead() - start;

        return new MaxKeyElement(name, size);
    }

    /**
     * Reads a {@link MinKeyElement} from the stream.
     * 
     * @return The {@link MinKeyElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link MinKeyElement}.
     */
    protected MinKeyElement readMinKeyElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();

        final long size = getBytesRead() - start;

        return new MinKeyElement(name, size);
    }

    /**
     * Reads a {@link MongoTimestampElement} from the stream.
     * 
     * @return The {@link MongoTimestampElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link MongoTimestampElement}.
     */
    protected MongoTimestampElement readMongoTimestampElement()
            throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final long timestamp = readLong();

        final long size = getBytesRead() - start;

        return new MongoTimestampElement(name, timestamp, size);
    }

    /**
     * Reads a {@link NullElement} from the stream.
     * 
     * @return The {@link NullElement}.
     * @throws IOException
     *             On a failure to read the contents of the {@link NullElement}.
     */
    protected NullElement readNullElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();

        final long size = getBytesRead() - start;

        return new NullElement(name, size);
    }

    /**
     * Reads a {@link ObjectIdElement} from the stream.
     * 
     * @return The {@link ObjectIdElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link ObjectIdElement}.
     */
    protected ObjectIdElement readObjectIdElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final int timestamp = EndianUtils.swap(readInt());
        final long machineId = EndianUtils.swap(readLong());

        final long size = getBytesRead() - start;

        return new ObjectIdElement(name, new ObjectId(timestamp, machineId),
                size);
    }

    /**
     * Reads a {@link RegularExpressionElement} from the stream.
     * 
     * @return The {@link RegularExpressionElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link RegularExpressionElement}.
     */
    protected RegularExpressionElement readRegularExpressionElement()
            throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final String pattern = readCString();
        final String options = readCString();

        final long size = getBytesRead() - start;

        return new RegularExpressionElement(name, pattern, options, size);
    }

    /**
     * Reads a "string" value from the stream:<code>
     * <pre>
     * string 	::= 	int32 (byte*) "\x00"
     * </pre>
     * </code>
     * <p>
     * <blockquote>String - The int32 is the number bytes in the (byte*) + 1
     * (for the trailing '\x00'). The (byte*) is zero or more UTF-8 encoded
     * characters. </blockquote>
     * </p>
     * 
     * @return The string value.
     * @throws EOFException
     *             On insufficient data for the integer.
     * @throws IOException
     *             On a failure reading the integer.
     */
    protected String readString() throws EOFException, IOException {
        final int length = readInt();

        if (ensureFetched(length) != length) {
            throw new EOFException();
        }

        final int offset = myBufferOffset;

        // Advance the buffer.
        myBufferOffset += length;

        return myStringDecoder.decode(myBuffer, offset, length);
    }

    /**
     * Reads a {@link StringElement} from the stream.
     * 
     * @return The {@link StringElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link StringElement}.
     */
    protected StringElement readStringElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final String value = readString();

        final long size = getBytesRead() - start;

        return new StringElement(name, value, size);
    }

    /**
     * Reads a {@link SymbolElement} from the stream.
     * 
     * @return The {@link SymbolElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link SymbolElement}.
     */
    protected SymbolElement readSymbolElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final String symbol = readString();

        final long size = getBytesRead() - start;

        return new SymbolElement(name, symbol, size);
    }

    /**
     * Reads a {@link TimestampElement} from the stream.
     * 
     * @return The {@link TimestampElement}.
     * @throws IOException
     *             On a failure to read the contents of the
     *             {@link TimestampElement}.
     */
    protected TimestampElement readTimestampElement() throws IOException {
        final long start = getBytesRead() - 1; // Token already read.

        final String name = readCString();
        final long time = readLong();

        final long size = getBytesRead() - start;

        return new TimestampElement(name, time, size);
    }

    /**
     * Fetch the requested number of bytes from the underlying stream. Returns
     * the number of bytes available in the buffer or the number of requested
     * bytes, which ever is smaller.
     * 
     * @param size
     *            The number of bytes to be read.
     * @return The smaller of the number of bytes requested or the number of
     *         bytes available in the buffer.
     * @throws IOException
     *             On a failure to read from the underlying stream.
     */
    private final int ensureFetched(final int size) throws IOException {
        return fetch(size, true);
    }

    /**
     * Fetch the requested number of bytes from the underlying stream. Returns
     * the number of bytes available in the buffer or the number of requested
     * bytes, which ever is smaller.
     * 
     * @param size
     *            The number of bytes to be read.
     * @param forceRead
     *            Determines if a read is forced to ensure the buffer contains
     *            the number of bytes.
     * @return The smaller of the number of bytes requested or the number of
     *         bytes available in the buffer.
     * @throws IOException
     *             On a failure to read from the underlying stream.
     */
    private final int fetch(final int size, final boolean forceRead)
            throws IOException {
        // See if we need to read more data.
        int available = availableInBuffer();
        if (available < size) {
            // Yes - we do.

            // Will the size fit in the existing buffer?
            if (myBuffer.length < size) {
                // Nope - grow the buffer to the needed size.
                final byte[] newBuffer = new byte[size];

                // Copy the existing content into the new buffer.
                System.arraycopy(myBuffer, myBufferOffset, newBuffer, 0,
                        available);
                myBuffer = newBuffer;
            }
            else if (0 < available) {
                // Compact the buffer.
                System.arraycopy(myBuffer, myBufferOffset, myBuffer, 0,
                        available);
            }

            // Reset the limit and offset...
            myBytesRead += myBufferOffset;
            myBufferOffset = 0;
            myBufferLimit = available;

            // Now read as much as possible to fill the buffer.
            int read;
            do {
                read = myInput.read(myBuffer, myBufferLimit, myBuffer.length
                        - myBufferLimit);
                if (0 < read) {
                    available += read;
                    myBufferLimit += read;
                }
            }
            while (forceRead && (0 <= read) && (available < size));

            return Math.min(size, available);
        }

        return size;
    }

    /**
     * Reads the complete set of bytes from the stream or throws an
     * {@link EOFException}.
     * 
     * @param buffer
     *            The buffer into which the data is read.
     * @param offset
     *            The offset to start writing into the buffer.
     * @param length
     *            The number of bytes to write into the buffer.
     * @exception EOFException
     *                If the input stream reaches the end before reading all the
     *                bytes.
     * @exception IOException
     *                On an error reading from the underlying stream.
     * @see DataInput#readFully(byte[], int, int)
     */
    private void readFully(final byte[] buffer, final int offset,
            final int length) throws EOFException, IOException {

        int read = Math.min(length, availableInBuffer());
        System.arraycopy(myBuffer, myBufferOffset, buffer, offset, read);
        myBufferOffset += read;

        // Read directly from the stream to avoid a copy.
        while (read < length) {
            final int count = myInput
                    .read(buffer, offset + read, length - read);
            if (count < 0) {
                throw new EOFException();
            }
            read += count;

            // Directly read bytes never hit the buffer.
            myBytesRead += read;
        }
    }
}
