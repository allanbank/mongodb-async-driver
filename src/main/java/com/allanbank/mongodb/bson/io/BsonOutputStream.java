/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import com.allanbank.mongodb.bson.Document;

/**
 * A wrapper for an {@link OutputStream} to handle writing BSON primitives.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonOutputStream {

    /** UTF-8 Character set for encoding strings. */
    public final static Charset UTF8 = Charset.forName("UTF-8");

    /** Output buffer for spooling the written document. */
    protected final OutputStream myOutput;

    /** Any thrown exceptions. */
    protected IOException myError;

    /** The visitor for writing BSON documents. */
    protected final UnbufferedWriteVisitor myWriteVisitor;

    /**
     * Creates a new {@link BsonOutputStream}.
     * 
     * @param output
     *            The underlying Stream to write to.
     */
    public BsonOutputStream(OutputStream output) {
        myOutput = output;
        myWriteVisitor = new UnbufferedWriteVisitor(this);
    }

    /**
     * Returns the I/O exception encountered by the visitor.
     * 
     * @return The I/O exception encountered by the visitor.
     */
    public IOException getError() {
        return myError;
    }

    /**
     * Returns true if the visitor had an I/O error.
     * 
     * @return True if the visitor had an I/O error, false otherwise.
     */
    public boolean hasError() {
        return (myError != null);
    }

    /**
     * Clears any errors.
     */
    public void reset() {
        myError = null;
    }

    /**
     * Writes a single byte to the stream.
     * 
     * @param b
     *            The byte to write.
     */
    public void writeByte(final byte b) {
        try {
            myOutput.write(b & 0xFF);
        } catch (IOException ioe) {
            myError = ioe;
        }
    }

    /**
     * Writes a sequence of bytes to the under lying stream.
     * 
     * @param data
     *            The bytes to write.
     */
    public void writeBytes(final byte[] data) {
        try {
            myOutput.write(data);
        } catch (IOException ioe) {
            myError = ioe;
        }
    }

    /**
     * Writes a "Cstring" to the stream.
     * 
     * @param strings
     *            The CString to write. The strings are concatenated into a
     *            single CString value.
     */
    public void writeCString(final String... strings) {
        for (final String string : strings) {
            writeBytes(string.getBytes(UTF8));
        }
        writeByte((byte) 0);
    }

    /**
     * Write the integer value in little-endian byte order.
     * 
     * @param value
     *            The integer to write.
     */
    public void writeInt(final int value) {
        try {
            myOutput.write((byte) (value & 0xFF));
            myOutput.write((byte) ((value >> 8) & 0xFF));
            myOutput.write((byte) ((value >> 16) & 0xFF));
            myOutput.write((byte) ((value >> 24) & 0xFF));
        } catch (IOException ioe) {
            myError = ioe;
        }
    }

    /**
     * Write the long value in little-endian byte order.
     * 
     * @param value
     *            The long to write.
     */
    public void writeLong(final long value) {
        try {
            myOutput.write((byte) (value & 0xFF));
            myOutput.write((byte) ((value >> 8) & 0xFF));
            myOutput.write((byte) ((value >> 16) & 0xFF));
            myOutput.write((byte) ((value >> 24) & 0xFF));
            myOutput.write((byte) ((value >> 32) & 0xFF));
            myOutput.write((byte) ((value >> 40) & 0xFF));
            myOutput.write((byte) ((value >> 48) & 0xFF));
            myOutput.write((byte) ((value >> 56) & 0xFF));
        } catch (IOException ioe) {
            myError = ioe;
        }
    }

    /**
     * Writes a "string" to the stream.
     * 
     * @param string
     *            The String to write.
     */
    public void writeString(final String string) {
        final byte[] bytes = string.getBytes(UTF8);

        writeInt(bytes.length + 1);
        writeBytes(bytes);
        writeByte((byte) 0);
    }

    /**
     * Writes a BSON {@link Document} to the stream.
     * 
     * @param document
     *            The {@link Document} to write.
     * @throws IOException
     *             On a failure writing the document.
     */
    public void writeDocument(final Document document) throws IOException {
        try {
            document.accept(myWriteVisitor);
            if (myWriteVisitor.hasError()) {
                throw myWriteVisitor.getError();
            }
        } finally {
            myWriteVisitor.reset();
        }
    }

    /**
     * Returns the size of the writing the {@link Document} as a BSON document.
     * 
     * @param document
     *            The document to determine the size of.
     * @return The size of the writing {@link Document} as a BSON document.
     */
    public int sizeOf(Document document) {
        return myWriteVisitor.sizeOf(document);
    }

    /**
     * Returns the size of the writing the <tt>strings</tt> as a c-string.
     * 
     * @param strings
     *            The 'C' strings to determine the size of.
     * @return The size of the writing the <tt>strings</tt> as a c-string.
     */
    public int sizeOfCString(String... strings) {
        int size = 0;
        for (String string : strings) {
            size += UTF8.encode(string).limit();
        }
        return (size + 1);
    }

    /**
     * Returns the size of the writing the <tt>string</tt> as a string.
     * 
     * @param string
     *            The 'UTF8' string to determine the size of.
     * @return The size of the writing the <tt>string</tt> as a string.
     */
    public int sizeOfString(String string) {
        return 4 + UTF8.encode(string).limit() + 1;
    }

}
