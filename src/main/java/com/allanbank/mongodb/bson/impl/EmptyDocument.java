/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;

/**
 * An immutable empty document.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class EmptyDocument implements Document {

    /** The empty list of elements. */
    public static final List<Element> EMPTY_ELEMENTS = Collections.emptyList();

    /** An instance of the Empty Document. */
    public static final EmptyDocument INSTANCE = new EmptyDocument();

    /** The hashCode for an empty {@link RootDocument}. */
    private static final int EMPTY_DOC_HASH = new RootDocument().hashCode();

    /** Serialization version for the class. */
    private static final long serialVersionUID = -2775918328146027036L;

    /**
     * Constructs a new {@link EmptyDocument}.
     */
    public EmptyDocument() {
        super();
    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitDocument} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visit(EMPTY_ELEMENTS);
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
     * Returns false since this document contains no {@link Element}s.
     * 
     * @see Document#contains(String)
     */
    @Override
    public boolean contains(final String name) {
        return false;
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
        else if (object != null) {
            if (getClass() == object.getClass()) {
                result = true;
            }
            else if (object instanceof RootDocument) {
                result = ((RootDocument) object).getElements().isEmpty();
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an empty list of elements.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public <E extends Element> List<E> find(final Class<E> clazz,
            final String... nameRegexs) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an empty list of elements.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public List<Element> find(final String... nameRegexs) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Return <code>null</code>.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public <E extends Element> E findFirst(final Class<E> clazz,
            final String... nameRegexs) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>null</code>.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public Element findFirst(final String... nameRegexs) {
        return null;
    }

    /**
     * Returns an empty list of elements.
     * 
     * @see Document#get(String)
     */
    @Override
    public <E extends Element> E get(final Class<E> clazz, final String name) {
        return null;
    }

    /**
     * Returns an empty list of elements.
     * 
     * @see Document#get(String)
     */
    @Override
    public Element get(final String name) {
        return null;
    }

    /**
     * Returns an empty list of elements.
     * 
     * @return The elements in the document.
     */
    @Override
    public List<Element> getElements() {
        return Collections.emptyList();
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        // This is set to match an empty RootDocument.
        return EMPTY_DOC_HASH;
    }

    /**
     * Returns an iterator over an empty list of elements.
     * 
     * @see Iterable#iterator()
     */
    @Override
    public Iterator<Element> iterator() {
        return EMPTY_ELEMENTS.iterator();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an empty list of elements.
     * </p>
     * 
     * @see Document#queryPath
     * @deprecated Use the {@link #find(Class,String...)} method instead. Will
     *             be removed after the 1.1.0 release.
     */
    @Override
    @Deprecated
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
            final String... nameRegexs) {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an empty list of elements.
     * </p>
     * 
     * @see Document#queryPath
     * @deprecated Use the {@link #find(Class,String...)} method instead. Will
     *             be removed after the 1.1.0 release.
     */
    @Override
    @Deprecated
    public List<Element> queryPath(final String... nameRegexs) {
        return Collections.emptyList();
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
        return "{}";
    }
}
