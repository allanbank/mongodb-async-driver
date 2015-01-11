/*
 * #%L
 * DocumentBuilder.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.bson.builder;

import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * Interface for a builder used to construct a BSON document.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@NotThreadSafe
public interface DocumentBuilder
        extends Builder, DocumentAssignable {
    /**
     * Adds a pre-built element to the document.
     *
     * @param element
     *            The element to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code element} is <code>null</code>.
     */
    public DocumentBuilder add(ElementAssignable element)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, boolean value)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code data} is <code>null</code>.
     */
    public DocumentBuilder add(String name, byte subType, byte[] data)
            throws IllegalArgumentException;

    /**
     * Adds a binary element using sub-type zero (the default).
     * <p>
     * This is a equivalent to {@link #addBinary(String,byte[])} but will insert
     * a {@link NullElement} if the {@code data} is <code>null</code> instead of
     * throwing an {@link IllegalArgumentException}.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, byte[] data)
            throws IllegalArgumentException;

    /**
     * Adds a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * <p>
     * This is a equivalent to {@link #addTimestamp(String,long)
     * addTimeStamp(timestamp.getTime())} but will insert a {@link NullElement}
     * if the {@code timestamp} is <code>null</code> instead of throwing an
     * {@link IllegalArgumentException}.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, Date timestamp)
            throws IllegalArgumentException;

    /**
     * Adds a pre-constructed document to the array.
     * <p>
     * This is a equivalent to {@link #addDocument(String,DocumentAssignable)}
     * but will insert a {@link NullElement} if the {@code document} is
     * <code>null</code> instead of throwing an {@link IllegalArgumentException}
     * .
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param document
     *            The document to add to the array.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, DocumentAssignable document)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, double value)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, int value)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, long value)
            throws IllegalArgumentException;

    /**
     * Adds the value to the document after trying to coerce the value into the
     * best possible element type. If the coercion fails then an
     * {@link IllegalArgumentException} is thrown.
     * <p>
     * This method does type inspection which can be slow. It is generally much
     * faster to use the type specific methods of this interface.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The Object value to coerce into an element.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code> or the {@code value}
     *             cannot be coerced into an element type.
     */
    public DocumentBuilder add(String name, Object value)
            throws IllegalArgumentException;

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addObjectId(String,ObjectId)} but will
     * insert a {@link NullElement} if the {@code id} is <code>null</code>
     * instead of throwing an {@link IllegalArgumentException}.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param id
     *            The ObjectId to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code id} is <code>null</code>.
     */
    public DocumentBuilder add(String name, ObjectId id)
            throws IllegalArgumentException;

    /**
     * Adds an ObjectId element.
     * <p>
     * This is a equivalent to {@link #addRegularExpression(String,Pattern)} but
     * will insert a {@link NullElement} if the {@code pattern} is
     * <code>null</code> instead of throwing an {@link IllegalArgumentException}
     * .
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, Pattern pattern)
            throws IllegalArgumentException;

    /**
     * Adds a string element.
     * <p>
     * This is a equivalent to {@link #addString(String,String)} but will insert
     * a {@link NullElement} if the {@code value} is <code>null</code> instead
     * of throwing an {@link IllegalArgumentException}.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, String value)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code databaseName},
     *             {@code collectionName}, or {@code id} is <code>null</code>.
     *
     * @deprecated See BSON specification.
     */
    @Deprecated
    public DocumentBuilder add(String name, String databaseName,
            String collectionName, ObjectId id) throws IllegalArgumentException;

    /**
     * Adds a (sub-type 4) {@link UUID} binary element.
     * <p>
     * This is a equivalent to {@link #addUuid(String,UUID)} but will insert a
     * {@link NullElement} if the {@code uuid} is <code>null</code> instead of
     * throwing an {@link IllegalArgumentException}.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param uuid
     *            The {@link UUID} to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder add(String name, UUID uuid)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code data} is <code>null</code>.
     */
    public DocumentBuilder addBinary(String name, byte subType, byte[] data)
            throws IllegalArgumentException;

    /**
     * Adds a binary element using sub-type zero (the default).
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code data} is <code>null</code>. If you would prefer a
     * {@link NullElement} be inserted in the document use the
     * {@link #add(String, byte[])} method instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param data
     *            The binary value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public DocumentBuilder addBinary(String name, byte[] data)
            throws IllegalArgumentException;

    /**
     * Adds a boolean element.
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The boolean value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addBoolean(String name, boolean value)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code databaseName},
     *             {@code collectionName}, or {@code id} is <code>null</code>.
     *
     * @deprecated See BSON specification.
     */
    @Deprecated
    public DocumentBuilder addDBPointer(String name, String databaseName,
            String collectionName, ObjectId id) throws IllegalArgumentException;

    /**
     * Adds a pre-built document element. Can also {@link #push(String)} a sub
     * document.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code value} is <code>null</code>. If you would prefer a
     * {@link NullElement} be inserted in the document use the
     * {@link #add(String, DocumentAssignable)} method instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The document value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public DocumentBuilder addDocument(String name, DocumentAssignable value)
            throws IllegalArgumentException;

    /**
     * Adds a double element.
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The double value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addDouble(String name, double value)
            throws IllegalArgumentException;

    /**
     * Adds a integer (32-bit signed) element.
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The integer value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addInteger(String name, int value)
            throws IllegalArgumentException;

    /**
     * Adds a JavaScript element.
     *
     * @param name
     *            The name of the element.
     * @param code
     *            The java script code.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code code} is <code>null</code>.
     */
    public DocumentBuilder addJavaScript(String name, String code)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name}, {@code value}, or {@code scope} is
     *             <code>null</code>.
     */
    public DocumentBuilder addJavaScript(String name, String code,
            DocumentAssignable scope) throws IllegalArgumentException;

    /**
     * Adds a legacy (sub-type 3) {@link UUID} binary element.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code uuid} is <code>null</code>.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param uuid
     *            The {@link UUID} to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code uuid} is <code>null</code>.
     */
    public DocumentBuilder addLegacyUuid(String name, UUID uuid)
            throws IllegalArgumentException;

    /**
     * Adds a long (64-bit signed) element.
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The long value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addLong(String name, long value)
            throws IllegalArgumentException;

    /**
     * Adds a minimum key value element. Used as an absolute upper bounds.
     *
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addMaxKey(String name)
            throws IllegalArgumentException;

    /**
     * Adds a minimum key value element. Used as an absolute lower bounds.
     *
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addMinKey(String name)
            throws IllegalArgumentException;

    /**
     * Adds a MongoDB Timestamp element.
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The mongoDB timstamp value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addMongoTimestamp(String name, long value)
            throws IllegalArgumentException;

    /**
     * Adds a <code>null</code> valued element.
     *
     * @param name
     *            The name of the element.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addNull(String name) throws IllegalArgumentException;

    /**
     * Adds an ObjectId element.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the {@code id}
     * is <code>null</code>. If you would prefer a {@link NullElement} be
     * inserted in the document use the {@link #add(String, ObjectId)} method
     * instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param id
     *            The ObjectId to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code id} is <code>null</code>.
     */
    public DocumentBuilder addObjectId(String name, ObjectId id)
            throws IllegalArgumentException;

    /**
     * Adds a regular expression element.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code pattern} is <code>null</code>. If you would prefer a
     * {@link NullElement} be inserted in the document use the
     * {@link #add(String, Pattern)} method instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     */
    public DocumentBuilder addRegularExpression(String name, Pattern pattern)
            throws IllegalArgumentException;

    /**
     * Adds a regular expression element.
     *
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @param options
     *            The regular expression options. See the BSON specification for
     *            details. The options may be <code>null</code>.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code pattern} is <code>null</code>.
     *             Note the {@code options} may be <code>null</code>.
     */
    public DocumentBuilder addRegularExpression(String name, String pattern,
            String options) throws IllegalArgumentException;

    /**
     * Adds a string element.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code value} is <code>null</code>. If you would prefer a
     * {@link NullElement} be inserted in the document use the
     * {@link #add(String, String)} method instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public DocumentBuilder addString(String name, String value)
            throws IllegalArgumentException;

    /**
     * Adds a symbol element.
     *
     * @param name
     *            The name of the element.
     * @param symbol
     *            The symbol value.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code symbol} is <code>null</code>.
     */
    public DocumentBuilder addSymbol(String name, String symbol)
            throws IllegalArgumentException;

    /**
     * Adds a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     *
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder addTimestamp(String name, long timestamp)
            throws IllegalArgumentException;

    /**
     * Adds a (sub-type 4) {@link UUID} binary element.
     * <p>
     * This method throws an {@link IllegalArgumentException} if the
     * {@code uuid} is <code>null</code>. If you would prefer a
     * {@link NullElement} be inserted in the document use the
     * {@link #add(String, UUID)} method instead.
     * </p>
     *
     * @param name
     *            The name of the element.
     * @param uuid
     *            The {@link UUID} to add.
     * @return This {@link DocumentBuilder} for method chaining.
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code uuid} is <code>null</code>.
     */
    public DocumentBuilder addUuid(String name, UUID uuid)
            throws IllegalArgumentException;

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentBuilder push(String name) throws IllegalArgumentException;

    /**
     * Pushes a context for constructing a sub-array.
     *
     * @param name
     *            The name of the sub-array.
     * @return A {@link ArrayBuilder} for constructing the sub-array.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public ArrayBuilder pushArray(String name) throws IllegalArgumentException;

    /**
     * Removes all {@link Element}s that have the provided name from the
     * document being built.
     * <p>
     * Note that adding a new element with the same name adds that element to
     * the end of document's element list.
     * </p>
     *
     * @param name
     *            The name of the element to remove.
     * @return This {@link DocumentBuilder} for method chaining.
     */
    public DocumentBuilder remove(String name);

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an {@link DocumentBuilder} instance.
     * </p>
     */
    @Override
    public DocumentBuilder reset();
}
