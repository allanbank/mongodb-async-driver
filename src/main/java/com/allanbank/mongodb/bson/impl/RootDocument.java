/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;

/**
 * A root level document that can inject a _id value.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RootDocument extends AbstractDocument {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2875918328146027036L;

    /**
     * Computes and returns the number of bytes that are used to encode the
     * document.
     * 
     * @param entries
     *            The entries in the document.
     * @return The size of the document when encoded in bytes.
     */
    private static long computeSize(final List<Element> entries) {
        long result = 5; // int length (4) + terminal null byte (1).
        if ((entries != null) && !entries.isEmpty()) {
            for (final Element element : entries) {
                result += element.size();
            }
        }

        return result;
    }

    /** The elements of the document. */
    final AtomicReference<List<Element>> myElements;

    /**
     * Tracks if the _id field is known to exist in the document when
     * constructed.
     */
    final boolean myIdKnownPresent;

    /**
     * Constructed when a user tries to access the elements of the document by
     * name.
     */
    private final AtomicReference<Map<String, Element>> myElementMap;

    /** The size of the document when encoded as bytes. */
    private transient long mySize;

    /**
     * Constructs a new {@link RootDocument}.
     * 
     * @param elements
     *            The elements for the BSON document.
     */
    public RootDocument(final Element... elements) {
        this(Arrays.asList(elements), false);
    }

    /**
     * Constructs a new {@link RootDocument}.
     * 
     * @param elements
     *            The elements for the BSON document.
     */
    public RootDocument(final List<Element> elements) {
        this(elements, false);
    }

    /**
     * Constructs a new {@link RootDocument}.
     * 
     * @param elements
     *            The elements for the BSON document.
     * @param idPresent
     *            If true then there is an _id element in the list of elements.
     */
    public RootDocument(final List<Element> elements, final boolean idPresent) {
        this(elements, idPresent, computeSize(elements));
    }

    /**
     * Constructs a new {@link RootDocument}.
     * 
     * @param elements
     *            The elements for the BSON document.
     * @param idPresent
     *            If true then there is an _id element in the list of elements.
     * @param size
     *            The size of the document when encoded in bytes. If not known
     *            then use the {@link RootDocument#RootDocument(List, boolean)}
     *            constructor instead.
     */
    public RootDocument(final List<Element> elements, final boolean idPresent,
            final long size) {
        myElements = new AtomicReference<List<Element>>();
        myElementMap = new AtomicReference<Map<String, Element>>();
        if ((elements != null) && !elements.isEmpty()) {
            myElements.set(Collections.unmodifiableList(new ArrayList<Element>(
                    elements)));
        }
        else {
            myElements.set(EMPTY_ELEMENTS);
        }
        myIdKnownPresent = idPresent;
        mySize = size;
    }

    /**
     * Returns true if the document contains an element with the specified name.
     * 
     * @see Document#contains(String)
     */
    @Override
    public boolean contains(final String name) {
        return (myIdKnownPresent && "_id".equals(name)) || super.contains(name);
    }

    /**
     * Returns the elements in the document.
     * 
     * @return The elements in the document.
     */
    @Override
    public List<Element> getElements() {
        return myElements.get();
    }

    /**
     * Adds an {@link ObjectIdElement} to the head of the document.
     */
    public void injectId() {
        if (!contains("_id")) {
            final List<Element> old = myElements.get();

            final ObjectIdElement toAdd = new ObjectIdElement("_id",
                    new ObjectId());

            final List<Element> newElements = new ArrayList<Element>(
                    old.size() + 1);
            newElements.add(toAdd);
            newElements.addAll(old);

            if (myElements.compareAndSet(old, newElements)) {
                myElementMap.set(null);
                mySize += toAdd.size();
            }
        }
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
        if (myElementMap.get() == null) {
            final List<Element> elements = myElements.get();
            final Map<String, Element> mapping = new HashMap<String, Element>(
                    elements.size() + elements.size());

            for (final Element element : elements) {
                mapping.put(element.getName(), element);
            }

            // Swap the finished map into position.
            myElementMap.compareAndSet(null, mapping);
        }

        return myElementMap.get();
    }
}
