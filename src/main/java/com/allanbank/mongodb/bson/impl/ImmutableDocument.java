/*
 * Copyright 2011-2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;

/**
 * A root level document that is truly immutable.
 * <p>
 * Documents normally returned from the document builders have the ability to be
 * modified by injecting an {@code _id} field into the document. This document
 * class does not have that ability.
 * </p>
 * <p>
 * Most users will not need to use this class except when creating static
 * documents within classes. The intended usage is then to use the builder when
 * constructing the immutable document: <blockquote>
 * 
 * <pre>
 * <code>
 * public static final Document QUERY;
 * 
 * ...
 * static {
 *    DocumentBuilder builder = BuilderFactory.start();
 * 
 *    builder.add(...);
 *    ...
 * 
 *    QUERY = new ImmutableDocument(builder);
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ImmutableDocument extends AbstractDocument {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2875918328146027037L;

    /**
     * Constructed when a user tries to access the elements of the document by
     * name.
     */
    private volatile Map<String, Element> myElementMap;

    /** The elements of the document. */
    private final List<Element> myElements;

    /** The size of the document when encoded as bytes. */
    private transient long mySize;

    /**
     * Constructs a new {@link ImmutableDocument}.
     * 
     * @param document
     *            The elements for the BSON document.
     */
    public ImmutableDocument(final DocumentAssignable document) {

        final List<Element> elements = document.asDocument().getElements();

        myElements = Collections.unmodifiableList(new ArrayList<Element>(
                elements));
        myElementMap = null;
        mySize = computeSize();
    }

    /**
     * Constructs a new {@link ImmutableDocument}.
     * 
     * @param document
     *            The elements for the BSON document.
     * @param size
     *            The size of the document when encoded in bytes. If not known
     *            then use the
     *            {@link ImmutableDocument#ImmutableDocument(DocumentAssignable)}
     *            constructor instead.
     */
    public ImmutableDocument(final DocumentAssignable document, final long size) {

        final List<Element> elements = document.asDocument().getElements();

        myElements = Collections.unmodifiableList(new ArrayList<Element>(
                elements));
        myElementMap = null;
        mySize = size;
    }

    /**
     * Returns the elements in the document.
     * 
     * @return The elements in the document.
     */
    @Override
    public List<Element> getElements() {
        return myElements;
    }

    /**
     * Returns the size of the document when encoded as bytes.
     * 
     * @return The size of the document when encoded as bytes.
     */
    @Override
    public long size() {
        return mySize;
    }

    /**
     * Returns a map from the element names to the elements in the document.
     * Used for faster by-name access.
     * 
     * @return The element name to element mapping.
     */
    @Override
    protected Map<String, Element> getElementMap() {
        if (myElementMap == null) {
            final Map<String, Element> mapping = new HashMap<String, Element>(
                    myElements.size() << 1);

            for (final Element element : myElements) {
                mapping.put(element.getName(), element);
            }

            // Swap the finished map into position.
            myElementMap = mapping;
        }

        return myElementMap;
    }

    /**
     * Computes and returns the length of the document in bytes.
     * 
     * @return The length of the document in bytes.
     */
    private long computeSize() {
        long result = 5; // int length (4) + terminal null byte (1).
        for (final Element element : myElements) {
            result += element.size();
        }

        return result;
    }

    /**
     * Sets the transient state of this document.
     * 
     * @param in
     *            The input stream.
     * @throws ClassNotFoundException
     *             On a failure loading a class in this classed reachable tree.
     * @throws IOException
     *             On a failure reading from the stream.
     */
    private void readObject(final ObjectInputStream in)
            throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        mySize = computeSize();
    }
}
