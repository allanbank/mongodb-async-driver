/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON array.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayElement extends AbstractElement {

    /** The BSON type for an array. */
    public static final ElementType TYPE = ElementType.ARRAY;

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
     */
    public ArrayElement(final String name, final Element... entries) {
        super(TYPE, name);

        myEntries = Collections.unmodifiableList(new ArrayList<Element>(Arrays
                .asList(entries)));
    }

    /**
     * Constructs a new {@link ArrayElement}.
     * 
     * @param name
     *            The name for the BSON array.
     * @param entries
     *            The entries in the array.
     */
    public ArrayElement(final String name, final List<Element> entries) {
        super(TYPE, name);

        if ((entries != null) && !entries.isEmpty()) {
            myEntries = Collections.unmodifiableList(new ArrayList<Element>(
                    entries));
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
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        result = (31 * result)
                + ((myEntries == null) ? 0 : myEntries.hashCode());
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
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
            final String... nameRegexs) {
        if (0 < nameRegexs.length) {
            final List<E> elements = new ArrayList<E>();
            final String nameRegex = nameRegexs[0];
            final String[] subNameRegexs = Arrays.copyOfRange(nameRegexs, 1,
                    nameRegexs.length);
            try {
                final Pattern pattern = Pattern.compile(nameRegex);
                for (final Element element : myEntries) {
                    if (pattern.matcher(element.getName()).matches()) {
                        elements.addAll(queryPath(clazz, subNameRegexs));
                    }
                }

            }
            catch (final PatternSyntaxException pse) {
                // Assume a non-pattern?
                for (final Element element : myEntries) {
                    if (nameRegex.equals(element.getName())) {
                        elements.addAll(queryPath(clazz, subNameRegexs));
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
     * String form of the object.
     * 
     * @return A human readable form of the object.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append('"');
        builder.append(getName());
        builder.append("\" : [ ");

        boolean first = true;
        for (final Element entry : myEntries) {
            if (!first) {
                builder.append(",\n");
            }
            builder.append(entry.toString());
            first = false;
        }
        builder.append("]\n");

        return builder.toString();
    }

}
