/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import java.util.Date;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * Interface for a builder used to construct a BSON array.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ArrayBuilder extends Builder {

    /**
     * Adds a boolean element.
     * <p>
     * This is a equivalent to {@link #addBoolean(boolean)} but less verbose.
     * </p>
     * 
     * @param value
     *            The boolean value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(boolean value);

    /**
     * Adds a binary element.
     * <p>
     * This is a equivalent to {@link #addBinary(byte, byte[])} but less
     * verbose.
     * </p>
     * 
     * @param subType
     *            The sub-type for the binary data.
     * @param data
     *            The binary value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(byte subType, byte[] data);

    /**
     * Adds a binary element using sub-type zero (the default).
     * <p>
     * This is a equivalent to {@link #addBinary(byte[])} but less verbose.
     * </p>
     * 
     * @param data
     *            The binary value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(byte[] data);

    /**
     * Adds a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * <p>
     * This is a equivalent to {@link #addTimestamp(long)
     * addTimeStamp(timestamp.getTime())} but less verbose.
     * </p>
     * 
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(Date timestamp);

    /**
     * Adds a pre-constructed document to the array.
     * <p>
     * This is a equivalent to {@link #addDocument(DocumentAssignable)} but less
     * verbose.
     * </p>
     * 
     * @param document
     *            The document to add to the array.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(DocumentAssignable document);

    /**
     * Adds a double element.
     * <p>
     * This is a equivalent to {@link #addDouble(double)} but less verbose.
     * </p>
     * 
     * @param value
     *            The double value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(double value);

    /**
     * Adds a pre-built element to the document.
     * 
     * @param element
     *            The element to add.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(ElementAssignable element);

    /**
     * Adds a integer (32-bit signed) element.
     * <p>
     * This is a equivalent to {@link #addInteger(int)} but less verbose.
     * </p>
     * 
     * @param value
     *            The integer value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(int value);

    /**
     * Adds a long (64-bit signed) element.
     * <p>
     * This is a equivalent to {@link #addLong(long)} but less verbose.
     * </p>
     * 
     * @param value
     *            The long value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(long value);

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addObjectId(ObjectId)} but less verbose.
     * </p>
     * 
     * @param id
     *            The ObjectId to add.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(ObjectId id);

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addRegularExpression(Pattern)} but less
     * verbose.
     * </p>
     * 
     * @param pattern
     *            The pattern for the regular expression.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(Pattern pattern);

    /**
     * Adds a string element.
     * <p>
     * This is a equivalent to {@link #addString(String)} but less verbose.
     * </p>
     * 
     * @param value
     *            The string value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder add(String value);

    /**
     * Adds a deprecated DBPointer element.
     * <p>
     * This is a equivalent to {@link #addDBPointer(String, String, ObjectId)}
     * but less verbose.
     * </p>
     * 
     * @param databaseName
     *            The name of the database containing the document.
     * @param collectionName
     *            The name of the collection containing the document.
     * @param id
     *            The id for the document.
     * @return This {@link ArrayBuilder} for method chaining.
     * 
     * @deprecated See BSON specification.
     */
    @Deprecated
    public ArrayBuilder add(String databaseName, String collectionName,
            ObjectId id);

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
     * Adds a binary element using sub-type zero (the default).
     * 
     * @param data
     *            The binary value.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder addBinary(byte[] data);

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
     * @param databaseName
     *            The name of the database containing the document.
     * @param collectionName
     *            The name of the collection containing the document.
     * @param id
     *            The id for the document.
     * @return This {@link ArrayBuilder} for method chaining.
     * 
     * @deprecated See BSON specification.
     */
    @Deprecated
    public ArrayBuilder addDBPointer(String databaseName,
            String collectionName, ObjectId id);

    /**
     * Adds a pre-constructed document to the array.
     * 
     * @param document
     *            The document to add to the array.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder addDocument(DocumentAssignable document);

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
    public ArrayBuilder addJavaScript(String code, DocumentAssignable scope);

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
     * @param id
     *            The ObjectId to add.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder addObjectId(ObjectId id);

    /**
     * Adds a regular expression element.
     * 
     * @param pattern
     *            The pattern for the regular expression.
     * @return This {@link ArrayBuilder} for method chaining.
     */
    public ArrayBuilder addRegularExpression(Pattern pattern);

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
     * Returns the array of {@link Element}s being constructed.
     * 
     * @return The constructed array of {@link Element}.
     */
    public Element[] build();

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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an {@link ArrayBuilder} instance.
     * </p>
     */
    @Override
    public ArrayBuilder reset();
}
