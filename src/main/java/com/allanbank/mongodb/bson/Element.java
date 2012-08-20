/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.io.Serializable;
import java.util.List;

/**
 * A common interface for the basic BSON types used to construct Documents and
 * arrays.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Element extends Serializable, ElementAssignable {

    /**
     * Accepts the visitor and calls the appropriate method on the visitor based
     * on the element type.
     * 
     * @param visitor
     *            The visitor for the element.
     */
    public void accept(Visitor visitor);

    /**
     * Returns the name for the BSON type.
     * 
     * @return The name for the BSON type.
     */
    public String getName();

    /**
     * Returns the type for the BSON type.
     * 
     * @return The type for the BSON type.
     */
    public ElementType getType();

    /**
     * Returns the elements matching the path of regular expressions.
     * 
     * @param <E>
     *            The type of element to match.
     * @param clazz
     *            The class of elements to match.
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The elements matching the path of regular expressions.
     */
    public <E extends Element> List<E> queryPath(Class<E> clazz,
            String... nameRegexs);

    /**
     * Returns the elements matching the path of regular expressions.
     * 
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The elements matching the path of regular expressions.
     */
    public List<Element> queryPath(String... nameRegexs);

    /**
     * Creates a new element with the same type and value as this element but
     * with the specified name. This is useful when creating a query across a
     * set of collections where the filed name changes in the collections but
     * the values must be identical.
     * 
     * @param name
     *            The new name for the element.
     * @return The created element.
     */
    public Element withName(String name);

}
