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
import com.allanbank.mongodb.bson.element.ObjectId;
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

    /** Tracks if an _id element is present. */
    private boolean myIdPresent;

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
    public DocumentBuilder add(final Element element) {
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
    public DocumentBuilder addBinary(final String name, final byte subType,
            final byte[] value) {
        return add(new BinaryElement(name, subType, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addBinary(final String name, final byte[] value) {
        return add(new BinaryElement(name, (byte) 0, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addBoolean(final String name, final boolean value) {
        return add(new BooleanElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public DocumentBuilder addDBPointer(final String name,
            final String databaseName, final String collectionName,
            final ObjectId id) {
        return add(new com.allanbank.mongodb.bson.element.DBPointerElement(
                name, databaseName, collectionName, id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addDocument(final String name, final Document value) {
        return add(new DocumentElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addDouble(final String name, final double value) {
        return add(new DoubleElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addInteger(final String name, final int value) {
        return add(new IntegerElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addJavaScript(final String name, final String code) {
        return add(new JavaScriptElement(name, code));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addJavaScript(final String name, final String code,
            final Document scope) {
        return add(new JavaScriptWithScopeElement(name, code, scope));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addLong(final String name, final long value) {
        return add(new LongElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMaxKey(final String name) {
        return add(new MaxKeyElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMinKey(final String name) {
        return add(new MinKeyElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addMongoTimestamp(final String name, final long value) {
        return add(new MongoTimestampElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addNull(final String name) {
        return add(new NullElement(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addObjectId(final String name, final ObjectId id) {
        return add(new ObjectIdElement(name, id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addRegularExpression(final String name,
            final String pattern, final String options) {
        return add(new RegularExpressionElement(name, pattern, options));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addString(final String name, final String value) {
        return add(new StringElement(name, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addSymbol(final String name, final String symbol) {
        return add(new SymbolElement(name, symbol));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentBuilder addTimestamp(final String name, final long timestamp) {
        return add(new TimestampElement(name, timestamp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document get() {
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
     * <p>
     * Overridden to return an {@link DocumentElement}.
     * </p>
     */
    @Override
    protected Element get(final String name) {
        return new DocumentElement(name, subElements());
    }
}
