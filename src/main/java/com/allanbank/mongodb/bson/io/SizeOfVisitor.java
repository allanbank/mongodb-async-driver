/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import java.nio.charset.Charset;
import java.util.IdentityHashMap;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A visitor to determine the size of the documents it visits. Intermediate
 * document sizes are cached for faster access later.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class SizeOfVisitor implements Visitor {
	/** UTF-8 Character set for encoding strings. */
	public final static Charset UTF8 = Charset.forName("UTF-8");

	/** The cache of document sizes. */
	private final IdentityHashMap<Object, Integer> myCachedSizes;

	/** The computed size. */
	private int mySize;

	/**
	 * Creates a new {@link SizeOfVisitor}.
	 */
	public SizeOfVisitor() {
		myCachedSizes = new IdentityHashMap<Object, Integer>();
	}

	/**
	 * Resets the size to zero and clears the cached documents. Use
	 * {@link #rewind()} to just set the size to zero and not clear the cached
	 * documents.
	 */
	public void reset() {
		mySize = 0;
		myCachedSizes.clear();
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

		Integer cached = myCachedSizes.get(elements);
		if (cached != null) {
			mySize += cached;
			return;
		}

		final int position = mySize;

		// int - 4
		// elements...
		// byte

		mySize += 4;
		for (final Element element : elements) {
			element.accept(this);
		}
		mySize += 1;

		myCachedSizes.put(elements, mySize - position);
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
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void visitDBPointer(final String name, final int timestamp,
			final long machineId) {
		mySize += 1;
		mySize += computeCStringSize(name);
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
	public void visitObjectId(final String name, final int timestamp,
			final long machineId) {
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
	 * Returns the visitor's output buffer.
	 * 
     * @param string
     *            The 'C' string to determine the size of.
	 * @return The visitor's output buffer.
	 */
	public int computeCStringSize(String string) {
		return UTF8.encode(string).limit() + 1;
	}

	/**
	 * Returns the visitor's output buffer.
	 * 
     * @param string
     *            The 'UTF8' string to determine the size of.
	 * @return The visitor's output buffer.
	 */
	public int computeStringSize(String string) {
		return 4 + UTF8.encode(string).limit() + 1;
	}

}