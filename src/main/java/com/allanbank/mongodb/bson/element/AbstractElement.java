/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;

/**
 * A base class for the basic BSON types.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractElement implements Element {

    /** The name for the BSON type. */
    private final String myName;

    /** The token for the concrete BSON type. */
    private final ElementType myType;

    /**
     * Constructs a new {@link AbstractElement}.
     * 
     * @param type
     *            The type for the concrete BSON type.
     * @param name
     *            The name for the BSON type.
     */
    public AbstractElement(final ElementType type, final String name) {
        myType = type;
        myName = name;
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

            result = nullSafeEquals(myName, other.myName)
                    && nullSafeEquals(myType, other.myType);
        }
        return result;
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
     * Returns the type for the BSON type.
     * 
     * @return The type for the BSON type.
     */
    @Override
    public ElementType getType() {
        return myType;
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
        result = (31 * result) + ((myType == null) ? 0 : myType.hashCode());
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
    public <E extends Element> List<E> queryPath(final Class<E> clazz,
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
    public List<Element> queryPath(final String... nameRegexs) {
        return queryPath(Element.class, nameRegexs);
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
        return "AbstractElement [myName=" + myName + ", myType=" + myType + "]";
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
