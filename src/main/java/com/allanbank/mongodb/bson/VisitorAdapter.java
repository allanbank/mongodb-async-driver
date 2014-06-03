/*
 * #%L
 * VisitorAdapter.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson;

import java.util.List;

import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * VisitorAdapter provides a helper for {@link Visitor} implementations that are
 * only interested in a subset of the elements within a document.
 * <p>
 * This implementation will walk the entire tree of elements. Derived classes
 * need only override the methods of interest. Derived classes should be careful
 * to always call the {@code super} implementation of the following methods to
 * ensure they do not break the document walking. Calling {@code super} for all
 * methods is encouraged.
 * <ul>
 * <li> {@link #visit(List)}</li>
 * <li> {@link #visitArray(String, List)}</li>
 * <li> {@link #visitDocument(String, List)}</li>
 * <li> {@link #visitJavaScript(String, String, Document)}</li>
 * </ul>
 * </p>
 * <p>
 * As a further aid to {@link Visitor} implementations only interested in the
 * names of elements, this class will call the {@link #visitName(String)} method
 * for each element visited.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VisitorAdapter implements Visitor {

    /**
     * Creates a new VisitorAdapter.
     */
    public VisitorAdapter() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to iterate over the elements of the document.
     * </p>
     */
    @Override
    public void visit(final List<Element> elements) {
        for (final Element element : elements) {
            element.accept(this);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element and then iterate over the
     * elements of the array.
     * </p>
     */
    @Override
    public void visitArray(final String name, final List<Element> elements) {
        visitName(name);
        for (final Element element : elements) {
            element.accept(this);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitBinary(final String name, final byte subType,
            final byte[] data) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitBoolean(final String name, final boolean value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitDBPointer(final String name, final String databaseName,
            final String collectionName, final ObjectId id) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element and then iterate over the
     * elements of the document element.
     * </p>
     */
    @Override
    public void visitDocument(final String name, final List<Element> elements) {
        visitName(name);
        for (final Element element : elements) {
            element.accept(this);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitDouble(final String name, final double value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitInteger(final String name, final int value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitJavaScript(final String name, final String code) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitJavaScript(final String name, final String code,
            final Document scope) {
        visitName(name);
        scope.accept(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitLong(final String name, final long value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitMaxKey(final String name) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitMinKey(final String name) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitMongoTimestamp(final String name, final long value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitNull(final String name) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitObjectId(final String name, final ObjectId id) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitRegularExpression(final String name, final String pattern,
            final String options) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitString(final String name, final String value) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitSymbol(final String name, final String symbol) {
        visitName(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to visit the name of the element.
     * </p>
     */
    @Override
    public void visitTimestamp(final String name, final long timestamp) {
        visitName(name);
    }

    /**
     * Extension point for {@link Visitor} implementation only interested in the
     * name of the elements.
     * 
     * @param name
     *            The name of the element.
     */
    protected void visitName(final String name) {
        // Nothing. Extension point.
    }
}
