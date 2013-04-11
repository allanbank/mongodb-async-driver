/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.util.PatternUtils;

/**
 * A wrapper for a BSON array.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayElement extends AbstractElement {

    /** The BSON type for an array. */
    public static final ElementType TYPE = ElementType.ARRAY;

    /** A cache of the names for the elements at an index to be used. */
    private static final String[] ourIndexes;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -7363294574214059703L;
    static {
        ourIndexes = new String[256];

        for (int i = 0; i < ourIndexes.length; ++i) {
            ourIndexes[i] = Integer.toString(i).intern();
        }
    }

    /**
     * Similar to the caching of Integer object values for a range we cache the
     * index names for an array's first 256 positions.
     * 
     * @param index
     *            The index for the array element.
     * @return The name of the value at that index.
     */
    public static final String nameFor(final int index) {
        if ((0 <= index) && (index < ourIndexes.length)) {
            return ourIndexes[index];
        }
        return Integer.toString(index);
    }

    /**
     * The entries in the array. The name attribute will be ignored when
     * encoding the elements.
     */
    private final List<Element> myEntries;

    /**
     * Constructs a new {@link ArrayElement}.
     * 
     * @param name
     *            The name for the BSON array.
     * @param entries
     *            The entries in the array.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public ArrayElement(final String name, final Element... entries)
            throws IllegalArgumentException {
        super(name);

        // The names of the elements have to be a specific value.
        final int length = entries.length;
        final List<Element> elements = new ArrayList<Element>(length);
        for (int i = 0; i < length; ++i) {
            elements.add(entries[i].withName(nameFor(i)));
        }

        myEntries = Collections.unmodifiableList(elements);
    }

    /**
     * Constructs a new {@link ArrayElement}.
     * 
     * @param name
     *            The name for the BSON array.
     * @param entries
     *            The entries in the array.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public ArrayElement(final String name, final List<Element> entries)
            throws IllegalArgumentException {
        super(name);

        if ((entries != null) && !entries.isEmpty()) {
            // The names of the elements have to be a specific value.
            final int length = entries.size();
            final List<Element> elements = new ArrayList<Element>(length);
            int index = 0;
            for (final Element element : entries) {
                elements.add(element.withName(nameFor(index)));
                index += 1;
            }

            myEntries = Collections.unmodifiableList(elements);
        }
        else {
            myEntries = Collections.emptyList();
        }
    }

    /**
     * Accepts the visitor and calls the
     * {@link Visitor#visitArray(String, List)} method.
     * 
     * @see Element#accept(Visitor)
     */
    @Override
    public void accept(final Visitor visitor) {
        visitor.visitArray(getName(), getEntries());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the elements of the array if the base class
     * comparison is equals.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = super.compareTo(otherElement);

        if (result == 0) {
            final ArrayElement other = (ArrayElement) otherElement;
            final int length = Math.min(myEntries.size(),
                    other.myEntries.size());
            for (int i = 0; i < length; ++i) {
                result = myEntries.get(i).compareTo(other.myEntries.get(i));
                if (result != 0) {
                    return result;
                }
            }

            result = myEntries.size() - other.myEntries.size();
        }

        return result;
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
            final ArrayElement other = (ArrayElement) object;

            result = super.equals(object) && myEntries.equals(other.myEntries);
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
        if (0 < nameRegexs.length) {
            final List<E> elements = new ArrayList<E>();
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);
            try {
                final Pattern pattern = PatternUtils.toPattern(nameRegex);
                for (final Element element : myEntries) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(element.find(clazz, subNameRegexs));
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : myEntries) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(element.find(clazz, subNameRegexs));
                    }
                }
            }

            return elements;
        }

        // End of the path -- are we the right type
        if (clazz.isAssignableFrom(this.getClass())) {
            return Collections.singletonList(clazz.cast(this));
        }
        return Collections.emptyList();
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
                final Iterator<Element> iter = myEntries.iterator();
                while (iter.hasNext() && (element == null)) {
                    final Element docElement = iter.next();
                    if (pattern.matcher(docElement.getName()).matches()) {
                        element = docElement.findFirst(clazz, subNameRegexs);
                    }
                }
            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                final Iterator<Element> iter = myEntries.iterator();
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
     * Returns the entries in the array. The name attribute will be ignored when
     * encoding the elements. When decoded the names will be the strings 0, 1,
     * 2, 3, etc..
     * 
     * @return The entries in the array.
     */
    public List<Element> getEntries() {
        return myEntries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an Element[].
     * </p>
     */
    @Override
    public Element[] getValueAsObject() {
        return myEntries.toArray(new Element[myEntries.size()]);
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
        result = (31 * result) + myEntries.hashCode();
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new {@link ArrayElement}.
     * </p>
     */
    @Override
    public ArrayElement withName(final String name) {
        if (getName().equals(name)) {
            return this;
        }
        return new ArrayElement(name, myEntries);
    }

}
