/*
 * #%L
 * AbstractElement.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.bson.element;

import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.Element;

/**
 * A base class for the basic BSON types.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public abstract class AbstractElement
        implements Element {

    /** The base type (interface) for all elements. */
    protected static final Class<Element> ELEMENT_TYPE = Element.class;

    /** Serialization version for the class. */
    private static final long serialVersionUID = 7537761445549731633L;

    /**
     * Compares two {@code int} values numerically. The value returned is
     * identical to what would be returned by:
     *
     * <pre>
     * Integer.valueOf(x).compareTo(Integer.valueOf(y))
     * </pre>
     *
     * @param x
     *            the first {@code int} to compare
     * @param y
     *            the second {@code int} to compare
     * @return the value {@code 0} if {@code x == y}; a value less than
     *         {@code 0} if {@code x < y}; and a value greater than {@code 0} if
     *         {@code x > y}
     * @since 1.7
     */
    /* package */static int compare(final int x, final int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    /**
     * Compares two {@code long} values numerically. The value returned is
     * identical to what would be returned by:
     *
     * <pre>
     * Long.valueOf(x).compareTo(Long.valueOf(y))
     * </pre>
     *
     * @param x
     *            the first {@code long} to compare
     * @param y
     *            the second {@code long} to compare
     * @return the value {@code 0} if {@code x == y}; a value less than
     *         {@code 0} if {@code x < y}; and a value greater than {@code 0} if
     *         {@code x > y}
     * @since 1.7
     */
    /* package */static int compare(final long x, final long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    /** The name for the BSON type. */
    private final String myName;

    /** The size of the element when encoded in bytes. */
    private final long mySize;

    /**
     * Constructs a new {@link AbstractElement}.
     *
     * @param name
     *            The name for the BSON type.
     * @param size
     *            The size of the element when encoded in bytes.
     * @throws IllegalArgumentException
     *             If the {@code name} is <code>null</code>.
     */
    public AbstractElement(final String name, final long size)
            throws IllegalArgumentException {
        assertNotNull(name, "Cannot have an null name on an element.");

        myName = name;
        mySize = size;
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
     * {@inheritDoc}
     * <p>
     * Overridden to compare the elements based on the tuple (name, type).
     * Derived classes are responsible for the value portion of the full
     * comparison.
     * </p>
     */
    @Override
    public int compareTo(final Element otherElement) {
        int result = myName.compareTo(otherElement.getName());

        if (result == 0) {
            // Use the specialized comparison to match MongoDB's ordering of
            // types.
            result = getType().compare(otherElement.getType());
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
     * @see Element#find
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
     * @see Element#find
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
     * @see Element#findFirst
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
     * Returns the number of bytes that are used to encode the element.
     *
     * @return The bytes that are used to encode the element.
     */
    @Override
    public long size() {
        return mySize;
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
