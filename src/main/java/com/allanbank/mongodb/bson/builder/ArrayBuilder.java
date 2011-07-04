/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import com.allanbank.mongodb.bson.Document;

/**
 * Interface for a builder used to construct a BSON array.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ArrayBuilder extends Builder {

	/**
	 * Adds a binary element using sub-type zero (the default).
	 * 
	 * @param data
	 *            The binary value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addBinary(byte[] data);

	/**
	 * Adds a binary element.
	 * 
	 * @param subType
	 *            The sub-type for the binary data.
	 * @param data
	 *            The binary value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addBinary(byte subType, byte[] data);

	/**
	 * Adds a boolean element.
	 * 
	 * @param value
	 *            The boolean value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addBoolean(boolean value);

	/**
	 * Adds a deprecated DBPointer element.
	 * 
	 * @param timestamp
	 *            The timestamp from the DB pointer.
	 * @param machineId
	 *            The machine id from the DB pointer.
	 * @return This {@link ArrayBuilder} for method chaining.
	 * 
	 * @deprecated See BSON specification.
	 */
	@Deprecated
	public ArrayBuilder addDBPointer(int timestamp, long machineId);

	/**
	 * Adds a double element.
	 * 
	 * @param value
	 *            The double value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addDouble(double value);

	/**
	 * Adds a integer (32-bit signed) element.
	 * 
	 * @param value
	 *            The integer value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addInteger(int value);

	/**
	 * Adds a JavaScript element.
	 * 
	 * @param code
	 *            The java script code.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addJavaScript(String code);

	/**
	 * Adds a JavaScript with Scope element.
	 * 
	 * @param code
	 *            The java script code.
	 * @param scope
	 *            The scope for the JacaScript code.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addJavaScript(String code, Document scope);

	/**
	 * Adds a long (64-bit signed) element.
	 * 
	 * @param value
	 *            The long value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addLong(long value);

	/**
	 * Adds a minimum key value element. Used as an absolute upper bounds.
	 * 
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addMaxKey();

	/**
	 * Adds a minimum key value element. Used as an absolute lower bounds.
	 * 
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addMinKey();

	/**
	 * Adds a MongoDB Timestamp element.
	 * 
	 * @param value
	 *            The mongoDB timstamp value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addMongoTimestamp(long value);

	/**
	 * Adds a <code>null</code> valued element.
	 * 
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addNull();

	/**
	 * Adds an ObjectId element.
	 * 
	 * @param timestamp
	 *            The timestamp from the object id.
	 * @param machineId
	 *            The machine id from the object id.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addObjectId(int timestamp, long machineId);

	/**
	 * Adds a regular expression element.
	 * 
	 * @param pattern
	 *            The pattern for the regular expression.
	 * @param options
	 *            The regular expression options. See the BSON specification for
	 *            details.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addRegularExpression(String pattern, String options);

	/**
	 * Adds a string element.
	 * 
	 * @param value
	 *            The string value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addString(String value);

	/**
	 * Adds a symbol element.
	 * 
	 * @param symbol
	 *            The symbol value.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addSymbol(String symbol);

	/**
	 * Adds a timestamp element. The timestamp is the number of milliseconds
	 * since the Unix epoch.
	 * 
	 * @param timestamp
	 *            The number of milliseconds since the Unix epoch.
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder addTimestamp(long timestamp);

	/**
	 * Pushes a context for constructing a sub-document.
	 * 
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public DocumentBuilder push();

	/**
	 * Pushes a context for constructing a sub-array.
	 * 
	 * @return This {@link ArrayBuilder} for method chaining.
	 */
	public ArrayBuilder pushArray();

}
