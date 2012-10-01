/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

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
import com.allanbank.mongodb.bson.DocumentReference;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.impl.RootDocument;
import com.allanbank.mongodb.util.PatternUtils;

/**
 * Wraps a single BSON document that may contain nested documents.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentElement(final String name, final Collection<Element> elements) {

        super(name);

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
     * @throws IllegalArgumentException
     *             If the {@code name} or {@code value} is <code>null</code>.
     */
    public DocumentElement(final String name, final Document value) {
        super(name);

        assertNotNull(value, "Document element's sub-document cannot be null.");

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public DocumentElement(final String name, final Element... elements) {
        super(name);

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
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
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

        super(name);

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
     * Returns this sub-document as a {@link DocumentReference} if it conforms
     * to the MongoDB DBRef convention. Returns <code>null</code> otherwise.
     * <p>
     * A DocumentReference contains (order matters):
     * <ol>
     * <li>The name of the collection where the referenced document resides:
     * {@code $ref}.</li>
     * <li>The value of the _id field in the referenced document: {@code $id}.</li>
     * <li>The name of the database where the referenced document resides:
     * {@code $db} (Optional).</li>
     * </ol>
     * 
     * @return This sub-document as a {@link DocumentReference} if it conforms
     *         to the MongoDB DBRef convention. Returns <code>null</code>
     *         otherwise.
     * 
     * @see #isDocumentReference()
     * @see <a
     *      href="http://docs.mongodb.org/manual/applications/database-references/#dbref">MongoDB
     *      DBRef Information</a>
     */
    public DocumentReference asDocumentReference() {
        final int elementCount = myElements.size();
        if (elementCount == 2) {
            final Element element1 = myElements.get(0);
            final Element element2 = myElements.get(1);

            final String element1Name = element1.getName();
            final ElementType element1Type = element1.getType();
            final String element2Name = element2.getName();

            if (DocumentReference.COLLECTION_FIELD_NAME.equals(element1Name)
                    && DocumentReference.ID_FIELD_NAME.equals(element2Name)) {
                if (element1Type == ElementType.STRING) {
                    return new DocumentReference(
                            ((StringElement) element1).getValue(), element2);
                }
                else if (element1Type == ElementType.SYMBOL) {
                    return new DocumentReference(
                            ((SymbolElement) element1).getSymbol(), element2);
                }
            }
        }
        else if (myElements.size() == 3) {
            final Element element1 = myElements.get(0);
            final Element element2 = myElements.get(1);
            final Element element3 = myElements.get(2);

            final String element1Name = element1.getName();
            final ElementType element1Type = element1.getType();
            final String element2Name = element2.getName();
            final String element3Name = element3.getName();
            final ElementType element3Type = element3.getType();

            if (DocumentReference.COLLECTION_FIELD_NAME.equals(element1Name)
                    && DocumentReference.ID_FIELD_NAME.equals(element2Name)
                    && DocumentReference.DATABASE_FIELD_NAME
                            .equals(element3Name)) {
                if (element1Type == ElementType.STRING) {
                    if (element3Type == ElementType.STRING) {
                        return new DocumentReference(
                                ((StringElement) element3).getValue(),
                                ((StringElement) element1).getValue(), element2);
                    }
                    else if (element3Type == ElementType.SYMBOL) {
                        return new DocumentReference(
                                ((SymbolElement) element3).getSymbol(),
                                ((StringElement) element1).getValue(), element2);
                    }
                }
                else if (element1Type == ElementType.SYMBOL) {
                    if (element3Type == ElementType.STRING) {
                        return new DocumentReference(
                                ((StringElement) element3).getValue(),
                                ((SymbolElement) element1).getSymbol(),
                                element2);
                    }
                    else if (element3Type == ElementType.SYMBOL) {
                        return new DocumentReference(
                                ((SymbolElement) element3).getSymbol(),
                                ((SymbolElement) element1).getSymbol(),
                                element2);
                    }
                }
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the elements of the document if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final DocumentElement other = (DocumentElement) otherElement;
            final int length = Math.min(myElements.size(),
                    other.myElements.size());
            for (int i = 0; i < length; ++i) {
                result = myElements.get(i).compareTo(other.myElements.get(i));
                if (result != 0) {
                    return result;
                }
            }

            result = myElements.size() - other.myElements.size();
        }

        return result;
    }

    /**
     * Returns true if the document contains an element with the specified name.
     * 
     * @param name
     *            The name of the element to locate.
     * @return True if the document contains an element with the given name,
     *         false otherwise.
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
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     * 
     * @see Element#queryPath
     */
    @Override
    public <E extends Element> List<E> find(final Class<E> clazz,
            final String... nameRegexs) {
        List<E> elements = Collections.emptyList();

        if (0 < nameRegexs.length) {
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            elements = new ArrayList<E>();
            try {
                final Pattern pattern = PatternUtils.toPattern(nameRegex);
                for (final Element element : myElements) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(element.find(clazz, subNameRegexs));
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : myElements) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(element.find(clazz, subNameRegexs));
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
     * {@inheritDoc}
     * <p>
     * Searches this sub-elements for matching elements on the path and are of
     * the right type.
     * </p>
     * 
     * @see Document#queryPath
     */
    @Override
    public <E extends Element> E findFirst(final Class<E> clazz,
            final String... nameRegexs) {
        E element = null;
        if (0 < nameRegexs.length) {
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);

            try {
                final Pattern pattern = PatternUtils.toPattern(nameRegex);
                final Iterator<Element> iter = myElements.iterator();
                while (iter.hasNext() && (element == null)) {
                    final Element docElement = iter.next();
                    if (pattern.matcher(docElement.getName()).matches()) {
                        element = docElement.findFirst(clazz, subNameRegexs);
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                final Iterator<Element> iter = myElements.iterator();
                while (iter.hasNext() && (element == null)) {
                    final Element docElement = iter.next();
                    if (nameRegex.equals(docElement.getName())) {
                        element = docElement.findFirst(clazz, subNameRegexs);
                    }
                }
            }
        }
        else {
            // End of the path -- are we the right type/element?
            if (clazz.isAssignableFrom(this.getClass())) {
                element = clazz.cast(this);
            }
        }
        return element;
    }

    /**
     * Returns the element with the specified name and type or null if no
     * element with that name and type exists.
     * 
     * @param <E>
     *            The type of element to get.
     * @param clazz
     *            The class of element to get.
     * @param name
     *            The name of the element to locate.
     * @return The sub-element in the document with the given name or null if
     *         element exists with the given name.
     * @see Document#get(Class, String)
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
     * @param name
     *            The name of the element to locate.
     * @return The sub-element in the document with the given name or null if
     *         element exists with the given name.
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
    @Override
    public List<Element> getElements() {
        return myElements;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
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
     * Returns true if this sub-document conforms to the MongoDB DBRef
     * convention, false otherwise.
     * <p>
     * A DocumentReference contains (order matters):
     * <ol>
     * <li>The name (string or symbol) of the collection where the referenced
     * document resides: {@code $ref}.</li>
     * <li>The value of the _id field in the referenced document: {@code $id}.</li>
     * <li>The name (string or symbol) of the database where the referenced
     * document resides: {@code $db} (Optional).</li>
     * </ol>
     * 
     * @return True if this sub-document conforms to the MongoDB DBRef
     *         convention, false otherwise.
     * 
     * @see #asDocumentReference()
     * @see DocumentReference
     * @see <a
     *      href="http://docs.mongodb.org/manual/applications/database-references/#dbref">MongoDB
     *      DBRef Information</a>
     */
    public boolean isDocumentReference() {
        final int elementCount = myElements.size();
        if (elementCount == 2) {
            final Element element1 = myElements.get(0);
            final Element element2 = myElements.get(1);

            final String element1Name = element1.getName();
            final ElementType element1Type = element1.getType();
            final String element2Name = element2.getName();

            return DocumentReference.COLLECTION_FIELD_NAME.equals(element1Name)
                    && ((element1Type == ElementType.STRING) || (element1Type == ElementType.SYMBOL))
                    && DocumentReference.ID_FIELD_NAME.equals(element2Name);
        }
        else if (myElements.size() == 3) {
            final Element element1 = myElements.get(0);
            final Element element2 = myElements.get(1);
            final Element element3 = myElements.get(2);

            final String element1Name = element1.getName();
            final ElementType element1Type = element1.getType();
            final String element2Name = element2.getName();
            final String element3Name = element3.getName();
            final ElementType element3Type = element3.getType();

            return DocumentReference.COLLECTION_FIELD_NAME.equals(element1Name)
                    && ((element1Type == ElementType.STRING) || (element1Type == ElementType.SYMBOL))
                    && DocumentReference.ID_FIELD_NAME.equals(element2Name)
                    && DocumentReference.DATABASE_FIELD_NAME
                            .equals(element3Name)
                    && ((element3Type == ElementType.STRING) || (element3Type == ElementType.SYMBOL));

        }
        return false;
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
