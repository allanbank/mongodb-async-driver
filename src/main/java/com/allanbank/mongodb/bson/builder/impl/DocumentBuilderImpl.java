/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder.impl;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
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
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * A builder for BSON documents.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentBuilderImpl extends AbstractBuilder implements
		DocumentBuilder {

	/**
	 * Creates a new builder.
	 */
	public DocumentBuilderImpl() {
		this(null);
	}

	/**
	 * Creates a new builder.
	 * 
	 * @param outerScope
	 *            The outer document scope.
	 */
	public DocumentBuilderImpl(final AbstractBuilder outerScope) {
		super(outerScope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBinary(final String name, final byte subType,
			final byte[] value) {
		myElements.add(new BinaryElement(name, subType, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBinary(final String name, final byte[] value) {
		myElements.add(new BinaryElement(name, (byte) 0, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBoolean(final String name, final boolean value) {
		myElements.add(new BooleanElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	@Deprecated
	public DocumentBuilder addDBPointer(final String name, final int timestamp,
			final long machineId) {
		myElements.add(new com.allanbank.mongodb.bson.element.DBPointerElement(
				name, timestamp, machineId));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addDouble(final String name, final double value) {
		myElements.add(new DoubleElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addInteger(final String name, final int value) {
		myElements.add(new IntegerElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addJavaScript(final String name, final String code) {
		myElements.add(new JavaScriptElement(name, code));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addJavaScript(final String name, final String code,
			final Document scope) {
		myElements.add(new JavaScriptWithScopeElement(name, code, scope));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addLong(final String name, final long value) {
		myElements.add(new LongElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMaxKey(final String name) {
		myElements.add(new MaxKeyElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMinKey(final String name) {
		myElements.add(new MinKeyElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMongoTimestamp(final String name, final long value) {
		myElements.add(new MongoTimestampElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addNull(final String name) {
		myElements.add(new NullElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addObjectId(final String name, final int timestamp,
			final long machineId) {
		myElements.add(new ObjectIdElement(name, timestamp, machineId));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addRegularExpression(final String name,
			final String pattern, final String options) {
		myElements.add(new RegularExpressionElement(name, pattern, options));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addString(final String name, final String value) {
		myElements.add(new StringElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addSymbol(final String name, final String symbol) {
		myElements.add(new SymbolElement(name, symbol));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addTimestamp(final String name, final long timestamp) {
		myElements.add(new TimestampElement(name, timestamp));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Document get() {
		return new RootDocument(subElements());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder push(final String name) {
		return doPush(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ArrayBuilder pushArray(final String name) {
		return doPushArray(name);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return an {@link DocumentElement}.
	 * </p>
	 */
	@Override
	protected Element get(final String name) {
		return new DocumentElement(name, subElements());
	}
}
