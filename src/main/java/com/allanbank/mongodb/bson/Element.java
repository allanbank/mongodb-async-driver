/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.io.Serializable;
import java.util.List;

/**
 * A common interface for the basic BSON types used to construct Documents and
 * arrays.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Element extends Serializable, ElementAssignable,
        Comparable<Element> {

    /**
     * Accepts the visitor and calls the appropriate method on the visitor based
     * on the element type.
     * 
     * @param visitor
     *            The visitor for the element.
     */
    public void accept(Visitor visitor);

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the elements based on the tuple (name, type,
     * value).
     * </p>
     */
    @Override
    public int compareTo(Element otherElement);

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
    public <E extends Element> List<E> find(Class<E> clazz,
            String... nameRegexs);

    /**
     * Returns the elements matching the path of regular expressions.
     * 
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The elements matching the path of regular expressions.
     */
    public List<Element> find(String... nameRegexs);

    /**
     * Returns the first element matching the path of regular expressions.
     * 
     * @param <E>
     *            The type of element to match.
     * @param clazz
     *            The class of element to match.
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The first element matching the path of regular expressions.
     */
    public <E extends Element> E findFirst(Class<E> clazz, String... nameRegexs);

    /**
     * Returns the first element matching the path of regular expressions.
     * 
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The first element matching the path of regular expressions.
     */
    public Element findFirst(String... nameRegexs);

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
     * @deprecated Use the {@link #find(Class,String...)} method instead. Will
     *             be removed after the 1.1.0 release.
     */
    @Deprecated
    public <E extends Element> List<E> queryPath(Class<E> clazz,
            String... nameRegexs);

    /**
     * Returns the elements matching the path of regular expressions.
     * 
     * @param nameRegexs
     *            The path of regular expressions.
     * @return The elements matching the path of regular expressions.
     * @deprecated Use the {@link #find(String...)} method instead. Will be
     *             removed after the 1.1.0 release.
     */
    @Deprecated
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
