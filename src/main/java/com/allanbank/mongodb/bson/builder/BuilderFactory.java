/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.impl.AbstractBuilder;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Helper class for getting started with a builder. The use of this class is
 * encouraged to avoid direct references to the builder implementations.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuilderFactory {

    /**
     * Creates an ArrayElement after trying to coerce the values into the best
     * possible element type. If the coersion fails then an
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param values
     *            The Object values to coerce into an element.
     * @return The {@link ArrayElement} with the name {@code ""} and the
     *         provided values.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code> or the {@code value}
     *             cannot be coerced into an element type.
     */
    public static final ArrayElement a(final Object... values) {
        final ArrayBuilder subArray = BuilderFactory.startArray();
        for (final Object entry : values) {
            subArray.add(entry);
        }
        return new ArrayElement("", subArray.build());
    }

    /**
     * Helper method for creating static document structures.
     * 
     * @param elements
     *            The elements of the document. The elements may be Created
     *            using the {@link #e(String, Object)} and {@link #a(Object...)}
     *            methods.
     * @return The document builder seeded with the specified elements.
     */
    public static final DocumentBuilder d(final Element... elements) {
        return new DocumentBuilderImpl(new RootDocument(elements));
    }

    /**
     * Creates an element after trying to coerce the value into the best
     * possible element type. If the coersion fails then an
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The Object value to coerce into an element.
     * @return The element with the name and value.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code> or the {@code value}
     *             cannot be coerced into an element type.
     */
    public static final Element e(final String name, final Object value) {
        return AbstractBuilder.coerse(name, value);
    }

    /**
     * Creates a new {@link DocumentBuilder}.
     * 
     * @return The root level document builder.
     */
    public static DocumentBuilder start() {
        return new DocumentBuilderImpl();
    }

    /**
     * Creates a new {@link DocumentBuilder} to append more elements to an
     * existing document.
     * 
     * @param seedDocument
     *            The document to seed the builder with. The builder will
     *            contain the seed document elements plus any added/appended
     *            elements.
     * @return The root level document builder.
     */
    public static DocumentBuilder start(final DocumentAssignable seedDocument) {
        return new DocumentBuilderImpl(seedDocument);
    }

    /**
     * Creates a new {@link DocumentBuilder} to append more elements to an
     * existing set of documents.
     * 
     * @param seedDocuments
     *            The documents to seed the builder with. The builder will
     *            contain the seed document elements plus any added/appended
     *            elements.
     * @return The root level document builder.
     */
    public static DocumentBuilder start(
            final DocumentAssignable... seedDocuments) {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();
        for (final DocumentAssignable seedDocument : seedDocuments) {
            for (final Element element : seedDocument.asDocument()) {
                builder.remove(element.getName());
                builder.add(element);
            }
        }
        return builder;
    }

    /**
     * Creates a new {@link ArrayBuilder}.
     * 
     * @return The root level array builder.
     */
    public static ArrayBuilder startArray() {
        return new ArrayBuilderImpl();
    }

    /**
     * Creates a new builder factory.
     */
    private BuilderFactory() {
        // Nothing to do.
    }
}
