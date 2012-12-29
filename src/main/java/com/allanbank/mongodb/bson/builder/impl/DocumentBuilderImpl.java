/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder.impl;

import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
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
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * A builder for BSON documents.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentBuilderImpl extends AbstractBuilder implements
        DocumentBuilder {

    /** Tracks if an _id element is present. */
    private boolean myIdPresent;

    /**
     * Creates a new builder.
     */
    public DocumentBuilderImpl() {
        this((AbstractBuilder) null);
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
     * Creates a new builder.
     * 
     * @param seedDocument
     *            The document to seed the builder with. The builder will
     *            contain the seed document elements plus any added/appended
     *            elements.
     */
    public DocumentBuilderImpl(final DocumentAssignable seedDocument) {
        this((AbstractBuilder) null);

        final Document document = seedDocument.asDocument();
        myElements.addAll(document.getElements());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final ElementAssignable elementRef)
            throws IllegalArgumentException {
        final Element element = elementRef.asElement();
        myElements.add(element);
        if ("_id".equals(element.getName())) {
            myIdPresent = true;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final boolean value)
            throws IllegalArgumentException {
        return addBoolean(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final byte subType,
            final byte[] data) throws IllegalArgumentException {
        return addBinary(name, subType, data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final byte[] data)
            throws IllegalArgumentException {
        if (data == null) {
            return addNull(name);
        }
        return addBinary(name, data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final Date timestamp)
            throws IllegalArgumentException {
        if (timestamp == null) {
            return addNull(name);
        }
        return addTimestamp(name, timestamp.getTime());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name,
            final DocumentAssignable document) throws IllegalArgumentException {
        if (document == null) {
            return addNull(name);
        }
        return addDocument(name, document);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final double value)
            throws IllegalArgumentException {
        return addDouble(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final int value)
            throws IllegalArgumentException {
        return addInteger(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final long value)
            throws IllegalArgumentException {
        return addLong(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final Object value)
            throws IllegalArgumentException {
        add(coerse(name, value));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final ObjectId id)
            throws IllegalArgumentException {
        if (id == null) {
            return addNull(name);
        }
        return addObjectId(name, id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final Pattern pattern)
            throws IllegalArgumentException {
        if (pattern == null) {
            return addNull(name);
        }
        return addRegularExpression(name, pattern);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final String value)
            throws IllegalArgumentException {
        if (value == null) {
            return addNull(name);
        }
        return addString(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public DocumentBuilder add(final String name, final String databaseName,
            final String collectionName, final ObjectId id)
            throws IllegalArgumentException {
        return addDBPointer(name, databaseName, collectionName, id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder add(final String name, final UUID uuid)
            throws IllegalArgumentException {
        if (uuid == null) {
            return addNull(name);
        }
        return addUuid(name, uuid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addBinary(final String name, final byte subType,
            final byte[] value) throws IllegalArgumentException {
        return add(new BinaryElement(name, subType, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addBinary(final String name, final byte[] value)
            throws IllegalArgumentException {
        return add(new BinaryElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addBoolean(final String name, final boolean value)
            throws IllegalArgumentException {
        return add(new BooleanElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public DocumentBuilder addDBPointer(final String name,
            final String databaseName, final String collectionName,
            final ObjectId id) throws IllegalArgumentException {
        return add(new com.allanbank.mongodb.bson.element.DBPointerElement(
                name, databaseName, collectionName, id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addDocument(final String name,
            final DocumentAssignable value) throws IllegalArgumentException {
        return add(new DocumentElement(name, value.asDocument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addDouble(final String name, final double value)
            throws IllegalArgumentException {
        return add(new DoubleElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addInteger(final String name, final int value)
            throws IllegalArgumentException {
        return add(new IntegerElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addJavaScript(final String name, final String code)
            throws IllegalArgumentException {
        return add(new JavaScriptElement(name, code));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addJavaScript(final String name, final String code,
            final DocumentAssignable scope) throws IllegalArgumentException {
        return add(new JavaScriptWithScopeElement(name, code,
                scope.asDocument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addLegacyUuid(final String name, final UUID uuid)
            throws IllegalArgumentException {
        return add(new UuidElement(name, UuidElement.LEGACY_UUID_SUBTTYPE, uuid));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addLong(final String name, final long value)
            throws IllegalArgumentException {
        return add(new LongElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMaxKey(final String name)
            throws IllegalArgumentException {
        return add(new MaxKeyElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMinKey(final String name)
            throws IllegalArgumentException {
        return add(new MinKeyElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMongoTimestamp(final String name, final long value)
            throws IllegalArgumentException {
        return add(new MongoTimestampElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addNull(final String name)
            throws IllegalArgumentException {
        return add(new NullElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addObjectId(final String name, final ObjectId id)
            throws IllegalArgumentException {
        return add(new ObjectIdElement(name, id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addRegularExpression(final String name,
            final Pattern pattern) throws IllegalArgumentException {
        return add(new RegularExpressionElement(name, pattern));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addRegularExpression(final String name,
            final String pattern, final String options)
            throws IllegalArgumentException {
        return add(new RegularExpressionElement(name, pattern, options));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addString(final String name, final String value)
            throws IllegalArgumentException {
        return add(new StringElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addSymbol(final String name, final String symbol)
            throws IllegalArgumentException {
        return add(new SymbolElement(name, symbol));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addTimestamp(final String name, final long timestamp)
            throws IllegalArgumentException {
        return add(new TimestampElement(name, timestamp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addUuid(final String name, final UUID uuid)
            throws IllegalArgumentException {
        return add(new UuidElement(name, UuidElement.UUID_SUBTTYPE, uuid));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the result of {@link #build()}.
     * </p>
     * 
     * @see #build()
     */
    @Override
    public Document asDocument() {
        return build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document build() {
        return new RootDocument(subElements(), myIdPresent);
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
     */
    @Override
    public DocumentBuilder remove(final String name) {
        final Iterator<Element> iter = myElements.iterator();
        while (iter.hasNext()) {
            if (name.equals(iter.next().getName())) {
                iter.remove();
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder reset() {
        super.reset();
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an {@link DocumentElement}.
     * </p>
     */
    @Override
    protected Element build(final String name) {
        return new DocumentElement(name, subElements(), true);
    }
}
