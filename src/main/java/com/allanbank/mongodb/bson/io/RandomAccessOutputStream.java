/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a capability similar to the <tt>ByteArrayOutputStream</tt> but also
 * provides the ability to re-write portions of the stream already written and
 * to determine the current size (or position) of the written data.
 * <p>
 * Instead of allocating a single large byte array this implementation tracks a
 * group of (increasing in size) buffers. This should reduce the runtime cost of
 * buffer reallocations since it avoids the copy of contents from one buffer to
 * another.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RandomAccessOutputStream extends OutputStream {
    /** UTF-8 Character set for encoding strings. */
    public final static Charset UTF8;

    /** The maximum size buffer to allocate. Must be a power of 2. */
    private static final int BUFFER_SIZE;

    /** Mask for the buffer position within a specific buffer. */
    private static final int BUFFER_SIZE_MASK;

    /** The number of bits in the buffer size. */
    private static final int BUFFER_SIZE_SHIFT;

    static {
        UTF8 = Charset.forName("UTF-8");

        BUFFER_SIZE = Integer.highestOneBit(8192);
        BUFFER_SIZE_MASK = BUFFER_SIZE - 1;
        BUFFER_SIZE_SHIFT = Integer.numberOfTrailingZeros(BUFFER_SIZE);
    }

    /** The set of buffers allocated. */
    private final List<byte[]> myBuffers;

    /** The current buffer being written. */
    private byte[] myCurrentBuffer;

    /** The index of the current buffer. */
    private int myCurrentBufferIndex;

    /** The offset into the current buffer. */
    private int myCurrentBufferOffset;

    /**
     * Buffer for serialization of integer types. Not needed for normal integer
     * writes since the {@link RandomAccessOutputStream} will coalesce the
     * single byte writes but for the {@link RandomAccessOutputStream#writeAt}
     * operation a seek to the appropriate backing buffer is required. For large
     * documents the seeks could be significant. This buffer ensures there is
     * only 1 seek for each {@link #writeIntAt(long, int)}.
     */
    private final byte[] myIntegerBytes;

    /** The current buffer being written. */
    private long mySize;

    /** The offset into the current buffer. */
    private final StringEncoder myStringEncoder = new StringEncoder();

    /**
     * Creates a new {@link RandomAccessOutputStream}.
     */
    public RandomAccessOutputStream() {
        mySize = 0;
        myCurrentBufferOffset = 0;
        myCurrentBufferIndex = 0;
        myCurrentBuffer = new byte[BUFFER_SIZE];

        myBuffers = new ArrayList<byte[]>();
        myBuffers.add(myCurrentBuffer);

        myIntegerBytes = new byte[8];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        // Nothing.
    }

    /**
     * Returns the maximum number of strings that may have their encoded form
     * cached.
     *
     * @return The maximum number of strings that may have their encoded form
     *         cached.
     */
    public int getMaxCachedStringEntries() {
        return myStringEncoder.getMaxCacheEntries();
    }

    /**
     * Returns the maximum length for a string that the stream is allowed to
     * cache.
     *
     * @return The maximum length for a string that the stream is allowed to
     *         cache.
     */
    public int getMaxCachedStringLength() {
        return myStringEncoder.getMaxCacheLength();
    }

    /**
     * Returns the current position in the stream. This is equivalent to
     * {@link #getSize()}.
     *
     * @return The current position in the stream.
     */
    public long getPosition() {
        return getSize();
    }

    /**
     * Returns the number of bytes written to the stream.
     *
     * @return The current number of bytes written to the stream.
     */
    public long getSize() {
        return mySize;
    }

    /**
     * Resets the <code>size</code> of the buffer to zero. All buffers can be
     * re-used.
     */
    public void reset() {
        mySize = 0;
        myCurrentBufferOffset = 0;
        myCurrentBufferIndex = 0;
        myCurrentBuffer = myBuffers.get(0);
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
        myStringEncoder.setMaxCacheEntries(maxCacheEntries);
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
        myStringEncoder.setMaxCacheLength(maxlength);

    }

    /**
     * {@inheritDoc}
     *
     * @param buffer
     *            the data.
     */
    @Override
    public void write(final byte buffer[]) {
        write(buffer, 0, buffer.length);
    }

    /**
     * {@inheritDoc}
     *
     * @param buffer
     *            the data.
     * @param offset
     *            the start offset in the data.
     * @param length
     *            the number of bytes to write.
     */
    @Override
    public void write(final byte buffer[], final int offset, final int length) {
        if (buffer == null) {
            throw new NullPointerException();
        }
        else if ((offset < 0) || (offset > buffer.length) || (length < 0)
                || ((offset + length) > buffer.length)
                || ((offset + length) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        else if (length == 0) {
            return;
        }

        int wrote = 0;
        while (wrote < length) {
            if (myCurrentBuffer.length <= myCurrentBufferOffset) {
                nextBuffer();
            }

            final int available = myCurrentBuffer.length
                    - myCurrentBufferOffset;
            final int toWrite = Math.min(length - wrote, available);

            System.arraycopy(buffer, offset + wrote, myCurrentBuffer,
                    myCurrentBufferOffset, toWrite);

            myCurrentBufferOffset += toWrite;
            mySize += toWrite;
            wrote += toWrite;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final int b) {
        if (myCurrentBuffer.length <= myCurrentBufferOffset) {
            nextBuffer();
        }

        myCurrentBuffer[myCurrentBufferOffset] = (byte) b;
        myCurrentBufferOffset += 1;
        mySize += 1;
    }

    /**
     * Similar to {@link #write(byte[])} but allows a portion of the already
     * written buffer to be re-written.
     * <p>
     * Equivalent to <code>writeAt(position, buffer, 0, buffer.length);</code>.
     * </p>
     *
     * @param position
     *            The position to write at. This location should have already
     *            been written.
     * @param buffer
     *            the data.
     */
    public void writeAt(final long position, final byte buffer[]) {
        writeAt(position, buffer, 0, buffer.length);
    }

    /**
     * Similar to {@link #write(byte[], int, int)} but allows a portion of the
     * already written buffer to be re-written.
     *
     * @param position
     *            The position to write at. This location should have already
     *            been written.
     * @param buffer
     *            the data.
     * @param offset
     *            the start offset in the data.
     * @param length
     *            the number of bytes to write.
     */
    public void writeAt(final long position, final byte buffer[],
            final int offset, final int length) {
        if (buffer == null) {
            throw new NullPointerException();
        }
        else if ((offset < 0) || (offset > buffer.length) || (length < 0)
                || ((offset + length) > buffer.length)
                || ((offset + length) < 0) || ((position + length) > getSize())) {
            throw new IndexOutOfBoundsException();
        }
        else if (length == 0) {
            return;
        }

        // Find the start buffer.
        final long start = position & BUFFER_SIZE_MASK;
        int bufferIndex = (int) (position >> BUFFER_SIZE_SHIFT);
        byte[] internalBuffer = myBuffers.get(bufferIndex);

        // Write into the correct position.
        int wrote = 0;
        int internalOffset = (int) start;
        while (wrote < length) {
            if (internalBuffer.length <= internalOffset) {
                bufferIndex += 1;
                internalBuffer = myBuffers.get(bufferIndex);
                internalOffset = 0;
            }

            final int available = internalBuffer.length - internalOffset;
            final int toWrite = Math.min(length - wrote, available);

            System.arraycopy(buffer, offset + wrote, internalBuffer,
                    internalOffset, toWrite);

            internalOffset += toWrite;
            wrote += toWrite;
        }
    }

    /**
     * Similar to {@link #write(int)} but allows a portion of the already
     * written buffer to be re-written.
     *
     * @param position
     *            The position to write at. This location should have already
     *            been written.
     * @param b
     *            The byte value to write.
     */
    public void writeAt(final long position, final int b) {
        // Find the start buffer.
        final long start = position & BUFFER_SIZE_MASK;
        final int bufferIndex = (int) (position >> BUFFER_SIZE_SHIFT);
        final byte[] internalBuffer = myBuffers.get(bufferIndex);

        internalBuffer[(int) start] = (byte) b;
    }

    /**
     * Writes a single byte to the stream.
     *
     * @param b
     *            The byte to write.
     */
    public void writeByte(final byte b) {
        write(b & 0xFF);
    }

    /**
     * Writes a sequence of bytes to the under lying stream.
     *
     * @param data
     *            The bytes to write.
     */
    public void writeBytes(final byte[] data) {
        write(data);
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
            // writeBytes(string.getBytes(UTF8));
            try {
                myStringEncoder.encode(string, this);
            }
            catch (final IOException cannotHappen) {
                // We never throw so should not throw from the encoder.
                throw new IllegalStateException(
                        "Encoder should not throw when writing to a buffer.");
            }
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
        myIntegerBytes[0] = (byte) (value & 0xFF);
        myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
        myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
        myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);

        write(myIntegerBytes, 0, 4);
    }

    /**
     * Write the integer value in little-endian byte order at the specified
     * position in the stream.
     *
     * @param position
     *            The position in the stream to write the integer.
     * @param value
     *            The long to write.
     */
    public void writeIntAt(final long position, final int value) {
        myIntegerBytes[0] = (byte) (value & 0xFF);
        myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
        myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
        myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);

        writeAt(position, myIntegerBytes, 0, 4);
    }

    /**
     * Write the long value in little-endian byte order.
     *
     * @param value
     *            The long to write.
     */
    public void writeLong(final long value) {
        myIntegerBytes[0] = (byte) (value & 0xFF);
        myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
        myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
        myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);
        myIntegerBytes[4] = (byte) ((value >> 32) & 0xFF);
        myIntegerBytes[5] = (byte) ((value >> 40) & 0xFF);
        myIntegerBytes[6] = (byte) ((value >> 48) & 0xFF);
        myIntegerBytes[7] = (byte) ((value >> 56) & 0xFF);

        write(myIntegerBytes, 0, 8);
    }

    /**
     * Writes a "string" to the stream.
     *
     * @param string
     *            The String to write.
     */
    public void writeString(final String string) {
        // final byte[] bytes = string.getBytes(UTF8);
        //
        // writeInt(bytes.length + 1);
        // writeBytes(bytes);
        // writeByte((byte) 0);

        final long position = getPosition();
        writeInt(0); // For size.
        try {
            myStringEncoder.encode(string, this);
        }
        catch (final IOException cannotHappen) {
            // We never throw so should not throw from the encoder.
            throw new IllegalStateException(
                    "Encoder should not throw when writing to a buffer.");
        }
        writeByte((byte) 0);

        final int size = (int) (getPosition() - position - 4);
        writeIntAt(position, size);

    }

    /**
     * Writes the complete contents of this byte array output stream to the
     * specified output stream argument, as if by calling the output stream's
     * write method using <code>out.write(buf, 0, count)</code>.
     *
     * @param out
     *            the output stream to which to write the data.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void writeTo(final OutputStream out) throws IOException {
        for (int i = 0; i < myCurrentBufferIndex; ++i) {
            out.write(myBuffers.get(i));
        }
        out.write(myCurrentBuffer, 0, myCurrentBufferOffset);
    }

    /**
     * Allocates a new buffer to use.
     */
    protected void nextBuffer() {
        // Need a new buffer.
        myCurrentBufferIndex += 1;

        if (myCurrentBufferIndex < myBuffers.size()) {
            myCurrentBuffer = myBuffers.get(myCurrentBufferIndex);
        }
        else {
            myCurrentBuffer = new byte[BUFFER_SIZE];
            myBuffers.add(myCurrentBuffer);
        }

        myCurrentBufferOffset = 0;
    }
}
