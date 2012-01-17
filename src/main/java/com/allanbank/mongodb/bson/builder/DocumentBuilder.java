/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * Interface for a builder used to construct a BSON document.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface DocumentBuilder extends Builder {
    /**
     * Adds a pre-built element to the document.
     * 
     * @param element
     *            The element to add.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(Element element);

    /**
     * Adds a binary element using sub-type zero (the default).
     * 
     * @param name
     *            The name of the element.
     * @param subType
     *            The sub-type for the binary data.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addBinary(String name, byte subType, byte[] data);

    /**
     * Adds a binary element using sub-type zero (the default).
     * 
     * @param name
     *            The name of the element.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addBinary(String name, byte[] data);

    /**
     * Adds a boolean element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The boolean value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addBoolean(String name, boolean value);

    /**
     * Adds a deprecated DBPointer element.
     * 
     * @param name
     *            The name of the element.
     * @param databaseName
     *            The name of the database containing the document.
     * @param collectionName
     *            The name of the collection containing the document.
     * @param id
     *            The id for the document.
     * @return This {@link DocumentBuilder} for method chaining.
     * 
     * @deprecated See BSON specification.
     */
    @Deprecated
    public DocumentBuilder addDBPointer(String name, String databaseName,
            String collectionName, ObjectId id);

    /**
     * Adds a double element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The double value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addDouble(String name, double value);

    /**
     * Adds a integer (32-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The integer value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addInteger(String name, int value);

    /**
     * Adds a JavaScript element.
     * 
     * @param name
     *            The name of the element.
     * @param code
     *            The java script code.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addJavaScript(String name, String code);

    /**
     * Adds a JavaScript with Scope element.
     * 
     * @param name
     *            The name of the element.
     * @param code
     *            The java script code.
     * @param scope
     *            The scope for the JacaScript code.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addJavaScript(String name, String code,
            Document scope);

    /**
     * Adds a long (64-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The long value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addLong(String name, long value);

    /**
     * Adds a minimum key value element. Used as an absolute upper bounds.
     * 
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addMaxKey(String name);

    /**
     * Adds a minimum key value element. Used as an absolute lower bounds.
     * 
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addMinKey(String name);

    /**
     * Adds a MongoDB Timestamp element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The mongoDB timstamp value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addMongoTimestamp(String name, long value);

    /**
     * Adds a <code>null</code> valued element.
     * 
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addNull(String name);

    /**
     * Adds an ObjectId element.
     * 
     * @param name
     *            The name of the element.
     * @param id
     *            The ObjectId to add.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addObjectId(String name, ObjectId id);

    /**
     * Adds a regular expression element.
     * 
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @param options
     *            The regular expression options. See the BSON specification for
     *            details.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addRegularExpression(String name, String pattern,
            String options);

    /**
     * Adds a string element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addString(String name, String value);

    /**
     * Adds a symbol element.
     * 
     * @param name
     *            The name of the element.
     * @param symbol
     *            The symbol value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addSymbol(String name, String symbol);

    /**
     * Adds a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * 
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addTimestamp(String name, long timestamp);

    /**
     * Returns the {@link Document} being constructed.
     * 
     * @return The constructed {@link Document}.
     */
    public Document get();

    /**
     * Pushes a context for constructing a sub-document.
     * 
     * @param name
     *            The name of the sub-document.
     * @return A {@link DocumentBuilder} for constructing the sub-document.
     */
    public DocumentBuilder push(String name);

    /**
     * Pushes a context for constructing a sub-array.
     * 
     * @param name
     *            The name of the sub-array.
     * @return A {@link ArrayBuilder} for constructing the sub-array.
     */
    public ArrayBuilder pushArray(String name);
}
