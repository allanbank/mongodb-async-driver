/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.impl;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.util.PatternUtils;

/**
 * AbstractDocument provides a base class for all document implementations with
 * the common functionality.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractDocument implements Document {

    /** The empty list of elements. */
    public static final List<Element> EMPTY_ELEMENTS = Collections.emptyList();

    /** The base type (interface) for all elements. */
    protected static final Class<Element> ELEMENT_TYPE = Element.class;

    /** The serialization id for the class. */
    private static final long serialVersionUID = -425294885378885212L;

    /**
     * Creates a new AbstractDocument.
     */
    public AbstractDocument() {
        super();
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
        return getElementMap().containsKey(name);
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
        else if ((object != null) && (object instanceof Document)) {
            final Document other = (Document) object;

            result = getElements().equals(other.getElements());
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     *
     * @see Document#find
     */
    @Override
    public <E extends Element> List<E> find(final Class<E> clazz,
            final String... nameRegexs) {
        List<E> elements = Collections.emptyList();
        if (0 < nameRegexs.length) {
            final List<Element> docElements = getElements();
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            elements = new ArrayList<E>();
            try {
                final Pattern pattern = PatternUtils.toPattern(nameRegex);
                for (final Element element : docElements) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(element.find(clazz, subNameRegexs));
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : docElements) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(element.find(clazz, subNameRegexs));
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
     * @see Document#find
     */
    @Override
    public List<Element> find(final String... nameRegexs) {
        return find(ELEMENT_TYPE, nameRegexs);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     *
     * @see Document#findFirst
     */
    @Override
    public <E extends Element> E findFirst(final Class<E> clazz,
            final String... nameRegexs) {
        E element = null;
        if (0 < nameRegexs.length) {
            final List<Element> docElements = getElements();
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            try {
                final Pattern pattern = PatternUtils.toPattern(nameRegex);
                final Iterator<Element> iter = docElements.iterator();
                while (iter.hasNext() && (element == null)) {
                    final Element docElement = iter.next();
                    if (pattern.matcher(docElement.getName()).matches()) {
                        element = docElement.findFirst(clazz, subNameRegexs);
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                final Iterator<Element> iter = docElements.iterator();
                while (iter.hasNext() && (element == null)) {
                    final Element docElement = iter.next();
                    if (nameRegex.equals(docElement.getName())) {
                        element = docElement.findFirst(clazz, subNameRegexs);
                    }
                }
            }
        }

        // End of the path but we are a document?
        return element;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     *
     * @see Document#findFirst
     */
    @Override
    public Element findFirst(final String... nameRegexs) {
        return findFirst(ELEMENT_TYPE, nameRegexs);
    }

    /**
     * Returns the element with the specified name and type or null if no
     * element with that name and type exists.
     *
     * @see Document#get(String)
     */
    @Override
    public <E extends Element> E get(final Class<E> clazz, final String name) {
        final Element element = get(name);
        if ((element != null) && clazz.isAssignableFrom(element.getClass())) {
            return clazz.cast(element);
        }
        return null;
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
     * {@inheritDoc}
     */
    @Override
    public abstract List<Element> getElements();

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        return getElements().hashCode();
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
     * String form of the object.
     *
     * @return A human readable form of the object.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, false);

        accept(visitor);

        return writer.toString();
    }

    /**
     * Returns the mapping from the names of elements to the element.
     *
     * @return The mapping from the names of elements to the element.
     */
    protected abstract Map<String, Element> getElementMap();

}