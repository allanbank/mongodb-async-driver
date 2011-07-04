/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A visitor to write the BSON document to a {@link RandomAccessOutputStream}.
 * The BSON specification uses prefixed length integers in several locations.
 * The {@link RandomAccessOutputStream} allows those values to be re-written
 * with a single serialization pass.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class WriteVisitor implements Visitor {

	/** UTF-8 Character set for encoding strings. */
	public final static Charset UTF8 = Charset.forName("UTF-8");

	/**
	 * Buffer for serialization of integer types. Not needed for normal integer
	 * writes since the {@link RandomAccessOutputStream} will coalesce the
	 * single byte writes but for the {@link RandomAccessOutputStream#writeAt}
	 * operation a seek to the appropriate backing buffer is required. For large
	 * documents the seeks could be significant. This buffer ensures there is
	 * only 1 seek for each {@link #writeIntAt(long, int)}.
	 */
	private final byte[] myIntegerBytes;

	/** Output buffer for spooling the written document. */
	protected final RandomAccessOutputStream myOutputBuffer;

	/**
	 * Creates a new {@link WriteVisitor}.
	 */
	public WriteVisitor() {
		myOutputBuffer = new RandomAccessOutputStream();
		myIntegerBytes = new byte[8];
	}

	/**
	 * Return the current Size of the written document.
	 * 
	 * @return The current size of the encoded document.
	 */
	public long getSize() {
		return myOutputBuffer.getPosition();
	}

	/**
	 * Clears the internal buffer and prepares to write another document.
	 */
	public void reset() {
		myOutputBuffer.reset();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visit(final List<Element> elements) {
		final long position = myOutputBuffer.getPosition();

		writeInt(0);
		for (final Element element : elements) {
			element.accept(this);
		}
		writeByte((byte) 0);

		final int size = (int) (myOutputBuffer.getPosition() - position);
		writeIntAt(position, size);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitArray(final String name, final List<Element> elements) {
		writeByte(ElementType.ARRAY.getToken());
		writeCString(name);
		visit(elements);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitBinary(final String name, final byte subType,
			final byte[] data) {
		writeByte(ElementType.BINARY.getToken());
		writeCString(name);
		switch (subType) {
		case 2: {
			writeInt(data.length + 4);
			writeByte(subType);
			writeInt(data.length);
			writeBytes(data);
			break;

		}
		case 0:
		default:
			writeInt(data.length);
			writeByte(subType);
			writeBytes(data);
			break;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitBoolean(final String name, final boolean value) {

		if (value) {
			writeByte(ElementType.TRUE.getToken());
		} else {
			writeByte(ElementType.FALSE.getToken());
		}
		writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void visitDBPointer(final String name, final int timestamp,
			final long machineId) {
		writeByte(ElementType.DB_POINTER.getToken());
		writeCString(name);
		// Just to be complicated the Object ID is big endian.
		writeInt(timestamp);
		writeLong(machineId);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitDocument(final String name, final List<Element> elements) {
		writeByte(ElementType.DOCUMENT.getToken());
		writeCString(name);
		visit(elements);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitDouble(final String name, final double value) {
		writeByte(ElementType.DOUBLE.getToken());
		writeCString(name);
		writeLong(Double.doubleToLongBits(value));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitInteger(final String name, final int value) {
		writeByte(ElementType.INTEGER.getToken());
		writeCString(name);
		writeInt(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitJavaScript(final String name, final String code) {
		writeByte(ElementType.JAVA_SCRIPT.getToken());
		writeCString(name);
		writeString(code);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitJavaScript(final String name, final String code,
			final Document scope) {
		writeByte(ElementType.JAVA_SCRIPT_WITH_SCOPE.getToken());
		writeCString(name);

		final long start = myOutputBuffer.getPosition();
		writeInt(0);
		writeString(code);

		scope.accept(this);

		int size = (int) (myOutputBuffer.getPosition() - start);
		writeIntAt(start, size);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitLong(final String name, final long value) {
		writeByte(ElementType.LONG.getToken());
		writeCString(name);
		writeLong(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMaxKey(final String name) {
		writeByte(ElementType.MAX_KEY.getToken());
		writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMinKey(final String name) {
		writeByte(ElementType.MIN_KEY.getToken());
		writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMongoTimestamp(final String name, final long value) {
		writeByte(ElementType.MONGO_TIMESTAMP.getToken());
		writeCString(name);
		writeLong(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitNull(final String name) {
		writeByte(ElementType.NULL.getToken());
		writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitObjectId(final String name, final int timestamp,
			final long machineId) {
		writeByte(ElementType.OBJECT_ID.getToken());
		writeCString(name);
		// Just to be complicated the Object ID is big endian.
		writeInt(EndianUtils.swap(timestamp));
		writeLong(EndianUtils.swap(machineId));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitRegularExpression(final String name, final String pattern,
			final String options) {
		writeByte(ElementType.REGEX.getToken());
		writeCString(name);
		writeCString(pattern);
		writeCString(options);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitString(final String name, final String value) {
		writeByte(ElementType.STRING.getToken());
		writeCString(name);
		writeString(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitSymbol(final String name, final String symbol) {
		writeByte(ElementType.SYMBOL.getToken());
		writeCString(name);
		writeString(symbol);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitTimestamp(final String name, final long timestamp) {
		writeByte(ElementType.UTC_TIMESTAMP.getToken());
		writeCString(name);
		writeLong(timestamp);
	}

	/**
	 * Writes the internal buffer to the provided stream.
	 * 
	 * @param out
	 *            The stream to write the internal buffer to.
	 * @throws IOException
	 *             On a failure writing the buffer to the <tt>out</tt> stream.
	 */
	public void writeTo(final OutputStream out) throws IOException {
		myOutputBuffer.writeTo(out);
	}

	/**
	 * Writes a single byte to the stream.
	 * 
	 * @param b
	 *            The byte to write.
	 */
	private void writeByte(final byte b) {
		myOutputBuffer.write(b & 0xFF);
	}

	/**
	 * Writes a sequence of bytes to the under lying stream.
	 * 
	 * @param data
	 *            The bytes to write.
	 */
	private void writeBytes(final byte[] data) {
		myOutputBuffer.write(data);
	}

	/**
	 * Writes a "Cstring" to the stream.
	 * 
	 * @param string
	 *            The CString to write.
	 */
	private void writeCString(final String string) {
		writeBytes(string.getBytes(UTF8));
		writeByte((byte) 0);
	}

	/**
	 * Write the integer value in little-endian byte order.
	 * 
	 * @param value
	 *            The integer to write.
	 */
	private void writeInt(final int value) {
		myIntegerBytes[0] = (byte) (value & 0xFF);
		myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
		myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
		myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);

		myOutputBuffer.write(myIntegerBytes, 0, 4);
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
	private void writeIntAt(final long position, final int value) {
		myIntegerBytes[0] = (byte) (value & 0xFF);
		myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
		myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
		myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);

		myOutputBuffer.writeAt(position, myIntegerBytes, 0, 4);
	}

	/**
	 * Write the long value in little-endian byte order.
	 * 
	 * @param value
	 *            The long to write.
	 */
	private void writeLong(final long value) {
		myIntegerBytes[0] = (byte) (value & 0xFF);
		myIntegerBytes[1] = (byte) ((value >> 8) & 0xFF);
		myIntegerBytes[2] = (byte) ((value >> 16) & 0xFF);
		myIntegerBytes[3] = (byte) ((value >> 24) & 0xFF);
		myIntegerBytes[4] = (byte) ((value >> 32) & 0xFF);
		myIntegerBytes[5] = (byte) ((value >> 40) & 0xFF);
		myIntegerBytes[6] = (byte) ((value >> 48) & 0xFF);
		myIntegerBytes[7] = (byte) ((value >> 56) & 0xFF);

		myOutputBuffer.write(myIntegerBytes, 0, 8);
	}

	/**
	 * Writes a "string" to the stream.
	 * 
	 * @param string
	 *            The String to write.
	 */
	private void writeString(final String string) {
		final byte[] bytes = string.getBytes(UTF8);

		writeInt(bytes.length + 1);
		writeBytes(bytes);
		writeByte((byte) 0);
	}

}