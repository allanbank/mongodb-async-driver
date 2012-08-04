/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Wraps a single BSON document that may contain nested documents.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentElement extends AbstractElement implements Document {

    /** The empty list of elements. */
    public static final List<Element> EMPTY_ELEMENTS = Collections.emptyList();

    /** The BSON type for a document. */
    public static final ElementType TYPE = ElementType.DOCUMENT;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -564259598403040796L;

    /**
     * Constructed when a user tries to access the elements of the document by
     * name.
     */
    private Map<String, Element> myElementMap;

    /** The elements of the document. */
    private final List<Element> myElements;

    /**
     * Constructs a new {@link DocumentElement}.
     * 
     * @param name
     *            The name for the BSON document.
     * @param elements
     *            The sub-elements for the document.
     */
    public DocumentElement(final String name, final Collection<Element> elements) {

        super(TYPE, name);

        if ((elements != null) && !elements.isEmpty()) {
            myElements = Collections.unmodifiableList(new ArrayList<Element>(
                    elements));
        }
        else {
            myElements = EMPTY_ELEMENTS;
        }
    }

    /**
     * Constructs a new {@link DocumentElement}.
     * 
     * @param name
     *            The name for the BSON document.
     * @param value
     *            The document to copy elements from.
     */
    public DocumentElement(final String name, final Document value) {
        super(TYPE, name);

        final List<Element> elements = new ArrayList<Element>();
        for (final Element element : value) {
            elements.add(element);
        }
        myElements = Collections.unmodifiableList(elements);
    }

    /**
     * Constructs a new {@link DocumentElement}.
     * 
     * @param name
     *            The name for the BSON document.
     * @param elements
     *            The sub-elements for the document.
     */
    public DocumentElement(final String name, final Element... elements) {
        super(TYPE, name);

        if (elements.length > 0) {
            myElements = Collections.unmodifiableList(new ArrayList<Element>(
                    Arrays.asList(elements)));
        }
        else {
            myElements = EMPTY_ELEMENTS;
        }
    }

    /**
     * Constructs a new {@link DocumentElement}.
     * 
     * @param name
     *            The name for the BSON document.
     * @param elements
     *            The sub-elements for the document.
     */
    public DocumentElement(final String name, final List<Element> elements) {
        this(name, elements, false);
    }

    /**
     * Constructs a new {@link DocumentElement}.
     * 
     * @param name
     *            The name for the BSON document.
     * @param elements
     *            The sub-elements for the document.
     * @param takeOwnership
     *            If true this element takes ownership of the list to avoid a
     *            copy of the list.
     */
    public DocumentElement(final String name, final List<Element> elements,
            final boolean takeOwnership) {

        super(TYPE, name);

        if ((elements != null) && !elements.isEmpty()) {
            if (takeOwnership) {
                myElements = Collections.unmodifiableList(elements);
            }
            else {
                myElements = Collections
                        .unmodifiableList(new ArrayList<Element>(elements));
            }
        }
        else {
            myElements = EMPTY_ELEMENTS;
        }

    }

    /**
     * Accepts the visitor and calls the {@link Visitor#visitDocument} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitDocument(getName(), getElements());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns this element.
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
        else if ((object != null) && (getClass() == object.getClass())) {
            final DocumentElement other = (DocumentElement) object;

            result = super.equals(object)
                    && myElements.equals(other.myElements);
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
     * Returns the element's document.
     * 
     * @return The document contained within the element.
     */
    public Document getDocument() {
        return new RootDocument(myElements);
    }

    /**
     * Returns the elements in the document.
     * 
     * @return The elements in the document.
     */
    public List<Element> getElements() {
        return myElements;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result) + myElements.hashCode();
        return result;
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
     * @see Element#queryPath
     */
    @Override
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
            final String... nameRegexs) {
        List<E> elements = Collections.emptyList();

        if (0 < nameRegexs.length) {
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            elements = new ArrayList<E>();
            try {
                final Pattern pattern = Pattern.compile(nameRegex);
                for (final Element element : myElements) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(element.queryPath(clazz, subNameRegexs));
                    }
                }

            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : myElements) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(element.queryPath(clazz, subNameRegexs));
                    }
                }
            }
        }
        else {
            // End of the path -- are we the right type/element?
            if (clazz.isAssignableFrom(this.getClass())) {
                elements = Collections.singletonList(clazz.cast(this));
            }
        }
        return elements;
    }

    /**
     * String form of the {@link DocumentElement}.
     * 
     * @return A human readable form of the {@link DocumentElement}.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append('"');
        builder.append(getName());
        builder.append("\" : { ");

        boolean first = true;
        for (final Element element : myElements) {
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
     * {@inheritDoc}
     * <p>
     * Returns a new {@link DocumentElement}.
     * </p>
     */
    @Override
    public DocumentElement withName(final String name) {
        return new DocumentElement(name, myElements);
    }

    /**
     * Returns a map from the element names to the elements in the document.
     * Used for faster by-name access.
     * 
     * @return The element name to element mapping.
     */
    private Map<String, Element> getElementMap() {
        if (myElementMap == null) {
            final List<Element> elements = myElements;
            final Map<String, Element> mapping = new HashMap<String, Element>(
                    elements.size() + elements.size());

            for (final Element element : elements) {
                mapping.put(element.getName(), element);
            }

            // Swap the finished map into position.
            myElementMap = mapping;
        }

        return myElementMap;
    }
}
