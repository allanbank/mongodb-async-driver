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
	public DocumentBuilderImpl(AbstractBuilder outerScope) {
		super(outerScope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBinary(String name, byte[] value) {
		myElements.add(new BinaryElement(name, (byte) 0, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBinary(String name, byte subType, byte[] value) {
		myElements.add(new BinaryElement(name, subType, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addBoolean(String name, boolean value) {
		myElements.add(new BooleanElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	@Deprecated
	public DocumentBuilder addDBPointer(String name, int timestamp,
			long machineId) {
		myElements.add(new com.allanbank.mongodb.bson.element.DBPointerElement(
				name, timestamp, machineId));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addDouble(String name, double value) {
		myElements.add(new DoubleElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addInteger(String name, int value) {
		myElements.add(new IntegerElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addJavaScript(String name, String code) {
		myElements.add(new JavaScriptElement(name, code));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addJavaScript(String name, String code,
			Document scope) {
		myElements.add(new JavaScriptWithScopeElement(name, code, scope));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addLong(String name, long value) {
		myElements.add(new LongElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMaxKey(String name) {
		myElements.add(new MaxKeyElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMinKey(String name) {
		myElements.add(new MinKeyElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addMongoTimestamp(String name, long value) {
		myElements.add(new MongoTimestampElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addNull(String name) {
		myElements.add(new NullElement(name));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addObjectId(String name, int timestamp,
			long machineId) {
		myElements.add(new ObjectIdElement(name, timestamp, machineId));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addRegularExpression(String name, String pattern,
			String options) {
		myElements.add(new RegularExpressionElement(name, pattern, options));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addString(String name, String value) {
		myElements.add(new StringElement(name, value));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addSymbol(String name, String symbol) {
		myElements.add(new SymbolElement(name, symbol));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DocumentBuilder addTimestamp(String name, long timestamp) {
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
	public DocumentBuilder push(String name) {
		return doPush(name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ArrayBuilder pushArray(String name) {
		return doPushArray(name);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return an {@link DocumentElement}.
	 * </p>
	 */
	@Override
	protected Element get(String name) {
		return new DocumentElement(name, subElements());
	}
}
