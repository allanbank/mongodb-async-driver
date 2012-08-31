/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import java.util.Date;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * Interface for a builder used to construct a BSON document.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface DocumentBuilder extends Builder, DocumentAssignable {
    /**
     * Adds a pre-built element to the document.
     * 
     * @param element
     *            The element to add.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(ElementAssignable element);

    /**
     * Adds a boolean element.
     * <p>
     * This is a equivalent to {@link #addBoolean(String,boolean)} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The boolean value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, boolean value);

    /**
     * Adds a binary element.
     * <p>
     * This is a equivalent to {@link #addBinary(String,byte, byte[])} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param subType
     *            The sub-type for the binary data.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, byte subType, byte[] data);

    /**
     * Adds a binary element using sub-type zero (the default).
     * <p>
     * This is a equivalent to {@link #addBinary(String,byte[])} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, byte[] data);

    /**
     * Adds a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * <p>
     * This is a equivalent to {@link #addTimestamp(String,long)
     * addTimeStamp(timestamp.getTime())} but less verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, Date timestamp);

    /**
     * Adds a pre-constructed document to the array.
     * <p>
     * This is a equivalent to {@link #addDocument(String,DocumentAssignable)}
     * but less verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param document
     *            The document to add to the array.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, DocumentAssignable document);

    /**
     * Adds a double element.
     * <p>
     * This is a equivalent to {@link #addDouble(String,double)} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The double value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, double value);

    /**
     * Adds a integer (32-bit signed) element.
     * <p>
     * This is a equivalent to {@link #addInteger(String,int)} but less verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The integer value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, int value);

    /**
     * Adds a long (64-bit signed) element.
     * <p>
     * This is a equivalent to {@link #addLong(String,long)} but less verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The long value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, long value);

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addObjectId(String,ObjectId)} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param id
     *            The ObjectId to add.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, ObjectId id);

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addRegularExpression(String,Pattern)} but
     * less verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, Pattern pattern);

    /**
     * Adds a string element.
     * <p>
     * This is a equivalent to {@link #addString(String,String)} but less
     * verbose.
     * </p>
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder add(String name, String value);

    /**
     * Adds a deprecated DBPointer element.
     * <p>
     * This is a equivalent to
     * {@link #addDBPointer(String,String, String, ObjectId)} but less verbose.
     * </p>
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
    public DocumentBuilder add(String name, String databaseName,
            String collectionName, ObjectId id);

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
     * Adds a pre-built document element. Can also {@link #push(String)} a sub
     * document.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The document value.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addDocument(String name, DocumentAssignable value);

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
            DocumentAssignable scope);

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
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder addRegularExpression(String name, Pattern pattern);

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
    public Document build();

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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an {@link DocumentBuilder} instance.
     * </p>
     */
    @Override
    public DocumentBuilder reset();
}
