/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.Builder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.AbstractElement;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;

/**
 * Base class with common functionality for the all builders. A builder is
 * responsible for constructing a single level of the BSON document.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
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
    public Builder reset() {
        myElements.clear();
        return this;
    }

    /**
     * Constructs the final form of the element being constructed.
     * 
     * @param name
     *            The name of the element.
     * @return The Element constructed by the builder.
     */
    protected abstract Element build(String name);

    /**
     * Performs type coersion on the value into the best possible element type.
     * If the coersion fails then an {@link IllegalArgumentException} is thrown.
     * <p>
     * This method does type inspection which can be slow. It is generally much
     * faster to use the type specific methods of this interface.
     * </p>
     * 
     * @param name
     *            The name for the element to create.
     * @param value
     *            The Object value to coerce into an element.
     * @return This {@link Element} resulting from the coersion.
     * @throws IllegalArgumentException
     *             If the {@code value} cannot be coerced into an element type.
     */
    protected Element coerse(final String name, final Object value)
            throws IllegalArgumentException {
        if (value == null) {
            return new NullElement(name);
        }
        else if (value instanceof Boolean) {
            return new BooleanElement(name, ((Boolean) value).booleanValue());
        }
        else if ((value instanceof Long) || (value instanceof BigInteger)) {
            return new LongElement(name, ((Number) value).longValue());
        }
        else if ((value instanceof Double) || (value instanceof Float)) {
            return new DoubleElement(name, ((Number) value).doubleValue());
        }
        else if (value instanceof Number) {
            return new IntegerElement(name, ((Number) value).intValue());
        }
        else if (value instanceof byte[]) {
            return new BinaryElement(name, (byte[]) value);
        }
        else if (value instanceof ObjectId) {
            return new ObjectIdElement(name, (ObjectId) value);
        }
        else if (value instanceof Pattern) {
            return new RegularExpressionElement(name, (Pattern) value);
        }
        else if (value instanceof String) {
            return new StringElement(name, (String) value);
        }
        else if (value instanceof Date) {
            return new TimestampElement(name, ((Date) value).getTime());
        }
        else if (value instanceof Calendar) {
            return new TimestampElement(name, ((Calendar) value).getTime()
                    .getTime());
        }
        else if (value instanceof UUID) {
            return new UuidElement(name, (UUID) value);
        }
        else if (value instanceof DocumentAssignable) {
            return new DocumentElement(name,
                    ((DocumentAssignable) value).asDocument());
        }
        else if (value instanceof ElementAssignable) {
            return ((ElementAssignable) value).asElement().withName(name);
        }
        else if (value instanceof Map) {
            final DocumentBuilder subDoc = BuilderFactory.start();
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                subDoc.add(entry.getKey().toString(), entry.getValue());
            }
            return new DocumentElement(name, subDoc.build());
        }
        else if (value instanceof Collection) {
            final ArrayBuilder subArray = BuilderFactory.startArray();
            for (final Object entry : (Collection<?>) value) {
                subArray.add(entry);
            }
            return new ArrayElement(name, subArray.build());
        }

        throw new IllegalArgumentException("Could not coerce the type '"
                + value.getClass().getName()
                + "' into a valid BSON element type.");
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
                element = ((BuilderElement) element).build();
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

        /** Serialization version for the class. */
        private static final long serialVersionUID = 4421203621373216989L;

        /** The encapsulated builder. */
        private transient AbstractBuilder myBuilder;

        /**
         * Creates a new {@link BuilderElement}.
         * 
         * @param name
         *            The name for the element to build.
         * @param builder
         *            The Builder doing the building.
         */
        public BuilderElement(final String name, final AbstractBuilder builder) {
            super(name);
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
        public Element build() {
            return myBuilder.build(getName());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ElementType getType() {
            return null;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns a new {@link BuilderElement}.
         * </p>
         */
        @Override
        public BuilderElement withName(final String name) {
            return new BuilderElement(name, myBuilder);
        }

        /**
         * Sets the transient state of this non-Element.
         * 
         * @param in
         *            The input stream.
         * @throws ClassNotFoundException
         *             On a failure loading a class in this classed reachable
         *             tree.
         * @throws IOException
         *             On a failure reading from the stream.
         */
        private void readObject(final ObjectInputStream in)
                throws ClassNotFoundException, IOException {
            in.defaultReadObject();
            myBuilder = null;
        }
    }
}
