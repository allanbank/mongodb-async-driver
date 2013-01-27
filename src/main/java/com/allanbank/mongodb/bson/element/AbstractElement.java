/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;

/**
 * A base class for the basic BSON types.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractElement implements Element {

    /** The base type (interface) for all elements. */
    protected static final Class<Element> ELEMENT_TYPE = Element.class;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7537761445549731633L;

    /** The name for the BSON type. */
    private final String myName;

    /**
     * Constructs a new {@link AbstractElement}.
     * 
     * @param name
     *            The name for the BSON type.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public AbstractElement(final String name) throws IllegalArgumentException {
        assertNotNull(name, "Cannot have an null name on an element.");

        myName = name;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns this element.
     * </p>
     */
    @Override
    public Element asElement() {
        return this;
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
            final AbstractElement other = (AbstractElement) object;

            result = nullSafeEquals(myName, other.myName);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a singleton list if the nameRegexs is empty and this element's
     * type is assignable to E. An empty list otherwise.
     * </p>
     * 
     * @see Element#queryPath
     */
    @Override
    public <E extends Element> List<E> find(final Class<E> clazz,
            final String... nameRegexs) {
        if ((nameRegexs.length == 0) && clazz.isAssignableFrom(this.getClass())) {
            // End of the path. Match this element.
            return Collections.singletonList(clazz.cast(this));
        }

        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a singleton list if the nameRegexs is empty. An empty list
     * otherwise.
     * </p>
     * 
     * @see Element#queryPath
     */
    @Override
    public List<Element> find(final String... nameRegexs) {
        return find(ELEMENT_TYPE, nameRegexs);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@code this} if the nameRegexs is empty and this element's type
     * is assignable to E. An empty list otherwise.
     * </p>
     * 
     * @see Element#findFirst
     */
    @Override
    public <E extends Element> E findFirst(final Class<E> clazz,
            final String... nameRegexs) {
        if ((nameRegexs.length == 0) && clazz.isAssignableFrom(this.getClass())) {
            // End of the path. Match this element.
            return clazz.cast(this);
        }

        return null;
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
    public Element findFirst(final String... nameRegexs) {
        return findFirst(ELEMENT_TYPE, nameRegexs);
    }

    /**
     * Returns the name for the BSON type.
     * 
     * @return The name for the BSON type.
     */
    @Override
    public String getName() {
        return myName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Uses the {@link JsonSerializationVisitor} to encode the value. In some
     * cases it will be more efficient to override this method with a more
     * straight forward conversion.
     * </p>
     */
    @Override
    public String getValueAsString() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, false);

        // Just the value.
        visitor.setSuppressNames(true);

        accept(visitor);

        return writer.toString();
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + ((myName == null) ? 0 : myName.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * To call the replacement method, {@link #find(Class, String...)}.
     * </p>
     * 
     * @see Document#queryPath
     * @deprecated Use the replacement method, {@link #find(Class, String...)}.
     *             Will be removed after the 1.1.0 release.
     */
    @Override
    @Deprecated
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
            final String... nameRegexs) {
        return find(clazz, nameRegexs);
    }

    /**
     * {@inheritDoc}
     * <p>
     * To call the replacement method, {@link #find(String...)}.
     * </p>
     * 
     * @see Document#queryPath
     * @deprecated Use the replacement method, {@link #find(String...)}. Will be
     *             removed after the 1.1.0 release.
     */
    @Override
    @Deprecated
    public List<Element> queryPath(final String... nameRegexs) {
        return find(nameRegexs);
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
     * Does a null safe equals comparison.
     * 
     * @param rhs
     *            The right-hand-side of the comparison.
     * @param lhs
     *            The left-hand-side of the comparison.
     * @return True if the rhs equals the lhs. Note: nullSafeEquals(null, null)
     *         returns true.
     */
    protected boolean nullSafeEquals(final Object rhs, final Object lhs) {
        return (rhs == lhs) || ((rhs != null) && rhs.equals(lhs));
    }
}
