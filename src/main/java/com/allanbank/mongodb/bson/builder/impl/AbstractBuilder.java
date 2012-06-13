/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.Builder;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.AbstractElement;

/**
 * Base class with common functionality for the all builders. A builder is
 * responsible for constructing a single level of the BSON document.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractBuilder implements Builder {

    /** If true then assertions have been enabled for the class. */
    protected static final boolean ASSERTIONS_ENABLED;

    /** The class used for intermediate sub-builders in the elements list. */
    protected static final Class<BuilderElement> BUILDER_ELEMENT_CLASS;

    static {
        BUILDER_ELEMENT_CLASS = BuilderElement.class;
        ASSERTIONS_ENABLED = AbstractBuilder.class.desiredAssertionStatus();
    }

    /** The list of elements in the builder. */
    protected final List<Element> myElements;

    /** The outer scope to this builder. */
    private final AbstractBuilder myOuterBuilder;

    /**
     * Creates a new builder.
     * 
     * @param outerBuilder
     *            The outer scoped builder.
     */
    public AbstractBuilder(final AbstractBuilder outerBuilder) {
        super();
        myOuterBuilder = outerBuilder;
        myElements = new ArrayList<Element>(5);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder pop() {
        return myOuterBuilder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        myElements.clear();
    }

    /**
     * Pushes a context for constructing a sub-document.
     * 
     * @param name
     *            The name of the sub-document.
     * @return A {@link DocumentBuilder} for constructing the sub-document.
     */

    protected DocumentBuilder doPush(final String name) {
        final DocumentBuilderImpl pushed = new DocumentBuilderImpl(this);
        myElements.add(new BuilderElement(name, pushed));
        return pushed;
    }

    /**
     * Pushes a context for constructing a sub-array.
     * 
     * @param name
     *            The name of the sub-array.
     * @return A {@link ArrayBuilder} for constructing the sub-array.
     */
    protected ArrayBuilder doPushArray(final String name) {
        final ArrayBuilderImpl pushed = new ArrayBuilderImpl(this);
        myElements.add(new BuilderElement(name, pushed));
        return pushed;
    }

    /**
     * Constructs the final form of the element being constructed.
     * 
     * @param name
     *            The name of the element.
     * @return The Element constructed by the builder.
     */
    protected abstract Element get(String name);

    /**
     * Renders the final form of the sub elements in the builder replacing all
     * {@link BuilderElement}s with the final element form.
     * 
     * @return The final sub element list.
     */
    protected List<Element> subElements() {
        final List<Element> elements = new ArrayList<Element>(myElements.size());

        Set<String> names = null;
        for (Element element : myElements) {
            if (element.getClass() == BUILDER_ELEMENT_CLASS) {
                element = ((BuilderElement) element).get();
            }

            if (ASSERTIONS_ENABLED) {
                if (names == null) {
                    names = new HashSet<String>(myElements.size() << 1);
                }
                final String name = element.getName();
                if (!names.add(name)) {
                    assert false : name + " is not unique in  " + myElements;
                }
            }

            elements.add(element);
        }

        return elements;
    }

    /**
     * A temporary Element to stand in for a element being constructed with a
     * builder.
     * <p>
     * <b>Note:</b> This class if final to allow the class comparison in
     * {@link AbstractBuilder}.subElements() method.
     * </p>
     */
    public static final class BuilderElement extends AbstractElement {

        /** The encapsulated builder. */
        private final AbstractBuilder myBuilder;

        /**
         * Creates a new {@link BuilderElement}.
         * 
         * @param name
         *            The name for the element to build.
         * @param builder
         *            The Builder doing the building.
         */
        public BuilderElement(final String name, final AbstractBuilder builder) {
            super(null, name);
            myBuilder = builder;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void accept(final Visitor visitor) {
            // No-op.
        }

        /**
         * Constructs the final form of the element being constructed by the
         * encapsulated builder.
         * 
         * @return The Element constructed by the encapsulated builder.
         */
        public Element get() {
            return myBuilder.get(getName());
        }
    }
}
