/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.util.List;

/**
 * A base class for the basic BSON types.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Element {

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
     * @param E
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

}
