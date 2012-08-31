/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;

/**
 * A root level document.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RootDocument implements Document {

    /** The empty list of elements. */
    public static final List<Element> EMPTY_ELEMENTS = Collections.emptyList();

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2875918328146027036L;

    /**
     * Constructed when a user tries to access the elements of the document by
     * name.
     */
    private final AtomicReference<Map<String, Element>> myElementMap;

    /** The elements of the document. */
    private final AtomicReference<List<Element>> myElements;

    /**
     * Tracks if the _id field is known to exist in the document when
     * constructed.
     */
    private final boolean myIdKnownPresent;

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
     * Accepts the visitor and calls the {@link Visitor#visitDocument} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visit(getElements());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns this document.
     * </p>
     */
    @Override
    public Document asDocument() {
        return this;
    }

    /**
     * Returns true if the document contains an element with the specified name.
     * 
     * @see Document#contains(String)
     */
    @Override
    public boolean contains(final String name) {
        return (myIdKnownPresent && "_id".equals(name))
                || getElementMap().containsKey(name);
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final RootDocument other = (RootDocument) object;

            result = myElements.get().equals(other.myElements.get());
        }
        return result;
    }

    /**
     * Returns the element with the specified name or null if no element with
     * that name exists.
     * 
     * @see Document#get(String)
     */
    @Override
    public Element get(final String name) {
        return getElementMap().get(name);
    }

    /**
     * Returns the elements in the document.
     * 
     * @return The elements in the document.
     */
    public List<Element> getElements() {
        return myElements.get();
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + myElements.get().hashCode();
        return result;
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
     * Returns an iterator over the documents elements.
     * 
     * @see Iterable#iterator()
     */
    @Override
    public Iterator<Element> iterator() {
        return getElements().iterator();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
            final String... nameRegexs) {
        List<E> elements = Collections.emptyList();
        if (0 < nameRegexs.length) {
            final List<Element> docElements = myElements.get();
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            elements = new ArrayList<E>();
            try {
                final Pattern pattern = Pattern.compile(nameRegex);
                for (final Element element : docElements) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(element.queryPath(clazz, subNameRegexs));
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : docElements) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(element.queryPath(clazz, subNameRegexs));
                    }
                }
            }
        }

        // End of the path but we are a document?
        return elements;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public List<Element> queryPath(final String... nameRegexs) {
        return queryPath(Element.class, nameRegexs);
    }

    /**
     * String form of the object.
     * 
     * @return A human readable form of the object.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("{ ");

        boolean first = true;
        for (final Element element : myElements.get()) {
            if (!first) {
                builder.append(",\n");
            }
            builder.append(element.toString());
            first = false;
        }
        builder.append("}\n");

        return builder.toString();
    }

    /**
     * Returns a map from the element names to the elements in the document.
     * Used for faster by-name access.
     * 
     * @return The element name to element mapping.
     */
    private Map<String, Element> getElementMap() {
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
