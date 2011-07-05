/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
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
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RandomAccessOutputStream extends OutputStream {

	/** The maximum size buffer to allocate. */
	private static final int MAX_BUFFER_SIZE = 8192;

	/** The minimum size buffer to allocate. */
	private static final int MIN_BUFFER_SIZE = 512;

	/** The set of buffers allocated. */
	private final List<byte[]> myBuffers;

	/** The current buffer being written. */
	private byte[] myCurrentBuffer;

	/** The index of the current buffer. */
	private int myCurrentBufferIndex;

	/** The offset into the current buffer. */
	private int myCurrentBufferOffset;

	/** The current buffer being written. */
	private long mySize;

	/**
	 * Creates a new {@link RandomAccessOutputStream}.
	 */
	public RandomAccessOutputStream() {
		mySize = 0;
		myCurrentBufferOffset = 0;
		myCurrentBufferIndex = 0;
		myCurrentBuffer = new byte[MIN_BUFFER_SIZE];

		myBuffers = new ArrayList<byte[]>();
		myBuffers.add(myCurrentBuffer);
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
		} else if ((offset < 0) || (offset > buffer.length) || (length < 0)
				|| ((offset + length) > buffer.length)
				|| ((offset + length) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (length == 0) {
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
		} else if ((offset < 0) || (offset > buffer.length) || (length < 0)
				|| ((offset + length) > buffer.length)
				|| ((offset + length) < 0) || ((position + length) > getSize())) {
			throw new IndexOutOfBoundsException();
		} else if (length == 0) {
			return;
		}

		// Find the start buffer.
		long start = position;
		int bufferIndex = 0;
		byte[] internalBuffer = myBuffers.get(bufferIndex);
		while (internalBuffer.length <= start) {
			start -= myBuffers.get(bufferIndex).length;
			bufferIndex += 1;
			internalBuffer = myBuffers.get(bufferIndex);
		}

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
		long start = position;
		int bufferIndex = 0;
		byte[] internalBuffer = myBuffers.get(bufferIndex);
		while (internalBuffer.length <= start) {
			start -= myBuffers.get(bufferIndex).length;
			bufferIndex += 1;
			internalBuffer = myBuffers.get(bufferIndex);
		}

		internalBuffer[(int) start] = (byte) b;
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
	 * 
	 */
	protected void nextBuffer() {
		// Need a new buffer.
		myCurrentBufferIndex += 1;

		if (myCurrentBufferIndex < myBuffers.size()) {
			myCurrentBuffer = myBuffers.get(myCurrentBufferIndex);
		} else {
			myCurrentBuffer = new byte[Math.min(myCurrentBuffer.length << 1,
					MAX_BUFFER_SIZE)];
			myBuffers.add(myCurrentBuffer);
		}

		myCurrentBufferOffset = 0;
	}
}
