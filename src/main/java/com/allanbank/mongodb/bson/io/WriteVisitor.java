/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * A visitor to write the BSON document to a {@link RandomAccessOutputStream}.
 * The BSON specification uses prefixed length integers in several locations.
 * The {@link RandomAccessOutputStream} allows those values to be re-written
 * with a single serialization pass.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class WriteVisitor implements Visitor {

	/** Output buffer for spooling the written document. */
	protected final RandomAccessOutputStream myOutputBuffer;

	/**
	 * Creates a new {@link WriteVisitor}.
	 */
	public WriteVisitor() {
		myOutputBuffer = new RandomAccessOutputStream();
	}

	/**
	 * Creates a new {@link WriteVisitor}.
	 * 
	 * @param output
	 *            The output buffer to use.
	 */
	public WriteVisitor(final RandomAccessOutputStream output) {
		myOutputBuffer = output;
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

		myOutputBuffer.writeInt(0);
		for (final Element element : elements) {
			element.accept(this);
		}
		myOutputBuffer.writeByte((byte) 0);

		final int size = (int) (myOutputBuffer.getPosition() - position);
		myOutputBuffer.writeIntAt(position, size);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitArray(final String name, final List<Element> elements) {
		myOutputBuffer.writeByte(ElementType.ARRAY.getToken());
		myOutputBuffer.writeCString(name);
		visit(elements);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitBinary(final String name, final byte subType,
			final byte[] data) {
		myOutputBuffer.writeByte(ElementType.BINARY.getToken());
		myOutputBuffer.writeCString(name);
		switch (subType) {
		case 2: {
			myOutputBuffer.writeInt(data.length + 4);
			myOutputBuffer.writeByte(subType);
			myOutputBuffer.writeInt(data.length);
			myOutputBuffer.writeBytes(data);
			break;

		}
		case 0:
		default:
			myOutputBuffer.writeInt(data.length);
			myOutputBuffer.writeByte(subType);
			myOutputBuffer.writeBytes(data);
			break;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitBoolean(final String name, final boolean value) {

		if (value) {
			myOutputBuffer.writeByte(ElementType.TRUE.getToken());
		} else {
			myOutputBuffer.writeByte(ElementType.FALSE.getToken());
		}
		myOutputBuffer.writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void visitDBPointer(final String name, String databaseName,
			String collectionName, final ObjectId id) {
		myOutputBuffer.writeByte(ElementType.DB_POINTER.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeString(databaseName + "." + collectionName); 
		// Just to be complicated the Object ID is big endian.
		myOutputBuffer.writeInt(EndianUtils.swap(id.getTimestamp()));
		myOutputBuffer.writeLong(EndianUtils.swap(id.getMachineId()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitDocument(final String name, final List<Element> elements) {
		myOutputBuffer.writeByte(ElementType.DOCUMENT.getToken());
		myOutputBuffer.writeCString(name);
		visit(elements);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitDouble(final String name, final double value) {
		myOutputBuffer.writeByte(ElementType.DOUBLE.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeLong(Double.doubleToLongBits(value));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitInteger(final String name, final int value) {
		myOutputBuffer.writeByte(ElementType.INTEGER.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeInt(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitJavaScript(final String name, final String code) {
		myOutputBuffer.writeByte(ElementType.JAVA_SCRIPT.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeString(code);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitJavaScript(final String name, final String code,
			final Document scope) {
		myOutputBuffer.writeByte(ElementType.JAVA_SCRIPT_WITH_SCOPE.getToken());
		myOutputBuffer.writeCString(name);

		final long start = myOutputBuffer.getPosition();
		myOutputBuffer.writeInt(0);
		myOutputBuffer.writeString(code);

		scope.accept(this);

		final int size = (int) (myOutputBuffer.getPosition() - start);
		myOutputBuffer.writeIntAt(start, size);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitLong(final String name, final long value) {
		myOutputBuffer.writeByte(ElementType.LONG.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeLong(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMaxKey(final String name) {
		myOutputBuffer.writeByte(ElementType.MAX_KEY.getToken());
		myOutputBuffer.writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMinKey(final String name) {
		myOutputBuffer.writeByte(ElementType.MIN_KEY.getToken());
		myOutputBuffer.writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitMongoTimestamp(final String name, final long value) {
		myOutputBuffer.writeByte(ElementType.MONGO_TIMESTAMP.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeLong(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitNull(final String name) {
		myOutputBuffer.writeByte(ElementType.NULL.getToken());
		myOutputBuffer.writeCString(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitObjectId(final String name, final ObjectId id) {
		myOutputBuffer.writeByte(ElementType.OBJECT_ID.getToken());
		myOutputBuffer.writeCString(name);
		// Just to be complicated the Object ID is big endian.
		myOutputBuffer.writeInt(EndianUtils.swap(id.getTimestamp()));
		myOutputBuffer.writeLong(EndianUtils.swap(id.getMachineId()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitRegularExpression(final String name, final String pattern,
			final String options) {
		myOutputBuffer.writeByte(ElementType.REGEX.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeCString(pattern);
		myOutputBuffer.writeCString(options);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitString(final String name, final String value) {
		myOutputBuffer.writeByte(ElementType.STRING.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeString(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitSymbol(final String name, final String symbol) {
		myOutputBuffer.writeByte(ElementType.SYMBOL.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeString(symbol);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitTimestamp(final String name, final long timestamp) {
		myOutputBuffer.writeByte(ElementType.UTC_TIMESTAMP.getToken());
		myOutputBuffer.writeCString(name);
		myOutputBuffer.writeLong(timestamp);
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
	 * Returns the visitor's output buffer.
	 * 
	 * @return The visitor's output buffer.
	 */
	protected RandomAccessOutputStream getOutputBuffer() {
		return myOutputBuffer;
	}

}