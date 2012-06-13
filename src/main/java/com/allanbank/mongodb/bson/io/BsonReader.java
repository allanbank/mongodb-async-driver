/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * {@link BsonReader} provides a class to read BSON documents based on the <a
 * href="http://bsonspec.org/">BSON specification</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonReader extends FilterInputStream {

    /** (U) UTF-8 Character set for encoding strings. */
    public final static Charset UTF8 = Charset.forName("UTF-8");

    /** A cached builder for strings. */
    private final StringBuilder myStringBuilder;

    /**
     * Creates a BSON document reader.
     * 
     * @param input
     *            the underlying stream to read from.
     */
    public BsonReader(final InputStream input) {
        super(input);
        myStringBuilder = new StringBuilder(64);
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
    @SuppressWarnings("null")
    public String readCString() throws EOFException, IOException {

        myStringBuilder.setLength(0);

        // Don't know how big the cstring is so have to
        // read a little, decode a little, read a little, ...
        CharsetDecoder decoder = null;
        ByteBuffer bytesIn = null;
        CharBuffer charBuffer = null;

        int read = read();
        while (read > 0) {
            if (bytesIn == null) {
                if (read < 0x80) {
                    // 1 byte encoded / ASCII!
                    myStringBuilder.append((char) read);
                }
                // else if (read < 0x800) {
                // // 2 byte encoded.
                // writeByte((byte) (0xc0 | (c >> 06)));
                // writeByte((byte) (0x80 | (c & 0x3f)));
                // }
                else {
                    // Complicated beyond here. Surrogates and what not. Let the
                    // full charset handle it.
                    decoder = UTF8.newDecoder();
                    bytesIn = ByteBuffer.allocate(64);
                    charBuffer = CharBuffer.allocate(64);
                    bytesIn = ByteBuffer.allocate(64);
                    bytesIn.put((byte) read);
                }
            }
            else {
                bytesIn.put((byte) read);

                if (!bytesIn.hasRemaining()) {

                    bytesIn.flip();
                    decoder.decode(bytesIn, charBuffer, false);
                    charBuffer.flip();
                    myStringBuilder.append(charBuffer);

                    charBuffer.clear();
                    bytesIn.compact();
                }
            }

            read = read();
        }

        if (read < 0) {
            throw new EOFException();
        }

        // Last decode.
        if (bytesIn != null) {
            bytesIn.flip();
            decoder.decode(bytesIn, charBuffer, true);
            charBuffer.flip();
            myStringBuilder.append(charBuffer);
        }

        return myStringBuilder.toString();
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
    public Document readDocument() throws EOFException, IOException {

        // The total length of the document. Not used.
        readInt();
        return new RootDocument(readElements());
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

        final int length = buffer.length;
        int index = 0;
        while (index < length) {
            final int count = in.read(buffer, index, length - index);
            if (count < 0) {
                throw new EOFException();
            }
            index += count;
        }
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
        int read = 0;
        int eofCheck = 0;
        int result = 0;

        for (int i = 0; i < Integer.SIZE; i += Byte.SIZE) {
            read = in.read();
            eofCheck |= read;
            result += (read << i);
        }

        if (eofCheck < 0) {
            throw new EOFException();
        }
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
        int read = 0;
        int eofCheck = 0;
        long result = 0;

        for (int i = 0; i < Long.SIZE; i += Byte.SIZE) {
            read = in.read();
            eofCheck |= read;
            result += (((long) read) << i);
        }

        if (eofCheck < 0) {
            throw new EOFException();
        }

        return result;
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
    protected ArrayElement readArrayElement() throws EOFException, IOException {

        final String name = readCString();

        // The total length of the document. Not used.
        readInt();

        return new ArrayElement(name, readElements());
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
     * @throws EOFException
     *             On insufficient data for the binary data.
     * @throws IOException
     *             On a failure reading the binary data.
     */
    protected BinaryElement readBinaryElement() throws EOFException,
            IOException {

        final String name = readCString();
        int length = readInt();

        final int subType = in.read();
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

        final byte[] binary = new byte[length];
        readFully(binary);

        return new BinaryElement(name, (byte) subType, binary);
    }

    /**
     * Reads a BSON Subdocument element: <code>
     * <pre>
     * "\x03" e_name document
     * </pre>
     * </code>
     * 
     * @return The {@link ArrayElement}.
     * @throws EOFException
     *             On insufficient data for the document.
     * @throws IOException
     *             On a failure reading the document.
     */
    protected DocumentElement readDocumentElement() throws EOFException,
            IOException {

        final String name = readCString();

        // The total length of the document. Not used.
        readInt();

        return new DocumentElement(name, readElements(), true);
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
     *             On insufficient data for the binary data.
     * @throws IOException
     *             On a failure reading the binary data.
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
            final String name = readCString();
            final String dbDotCollection = readString();
            String db = dbDotCollection;
            String collection = "";
            final int firstDot = dbDotCollection.indexOf('.');
            if (0 <= firstDot) {
                db = dbDotCollection.substring(0, firstDot);
                collection = dbDotCollection.substring(firstDot + 1);
            }
            return new com.allanbank.mongodb.bson.element.DBPointerElement(
                    name, db, collection, new ObjectId(
                            EndianUtils.swap(readInt()),
                            EndianUtils.swap(readLong())));
        }
        case DOCUMENT: {
            return readDocumentElement();
        }
        case DOUBLE: {
            return new DoubleElement(readCString(),
                    Double.longBitsToDouble(readLong()));
        }
        case BOOLEAN: {
            return new BooleanElement(readCString(), (read() == 1));
        }
        case INTEGER: {
            return new IntegerElement(readCString(), readInt());
        }
        case JAVA_SCRIPT: {
            return new JavaScriptElement(readCString(), readString());
        }
        case JAVA_SCRIPT_WITH_SCOPE: {
            final String name = readCString();

            // Total length - not used.
            readInt();

            return new JavaScriptWithScopeElement(name, readString(),
                    readDocument());
        }
        case LONG: {
            return new LongElement(readCString(), readLong());
        }
        case MAX_KEY: {
            return new MaxKeyElement(readCString());
        }
        case MIN_KEY: {
            return new MinKeyElement(readCString());
        }
        case MONGO_TIMESTAMP: {
            return new MongoTimestampElement(readCString(), readLong());
        }
        case NULL: {
            return new NullElement(readCString());
        }
        case OBJECT_ID: {
            return new ObjectIdElement(readCString(), new ObjectId(
                    EndianUtils.swap(readInt()), EndianUtils.swap(readLong())));
        }
        case REGEX: {
            return new RegularExpressionElement(readCString(), readCString(),
                    readCString());
        }
        case STRING: {
            return new StringElement(readCString(), readString());
        }
        case SYMBOL: {
            return new SymbolElement(readCString(), readString());
        }
        case UTC_TIMESTAMP: {
            return new TimestampElement(readCString(), readLong());
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
        int elementToken = in.read();
        while (elementToken > 0) {
            elements.add(readElement((byte) elementToken));

            elementToken = in.read();
        }
        if (elementToken < 0) {
            throw new EOFException();
        }
        return elements;
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

        final byte[] bytes = new byte[length];
        readFully(bytes);

        // Remember to remove the null byte at the end of the string.
        return new String(bytes, 0, bytes.length - 1, UTF8);
    }
}
