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

    /**
     * Constructs a new {@link RootDocument}.
     * 
     * @param elements
     *            The elements for the BSON document.
     */
    public RootDocument(final Element... elements) {
        myElements = new AtomicReference<List<Element>>();
        myElementMap = new AtomicReference<Map<String, Element>>();
        if (elements.length > 0) {
            myElements.set(Collections.unmodifiableList(new ArrayList<Element>(
                    Arrays.asList(elements))));
        }
        else {
            myElements.set(EMPTY_ELEMENTS);
        }
        myIdKnownPresent = false;
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

            final List<Element> newElements = new ArrayList<Element>();
            newElements.add(new ObjectIdElement("_id", new ObjectId()));
            newElements.addAll(old);

            if (myElements.compareAndSet(old, newElements)) {
                myElementMap.set(null);
            }
        }
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
