/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.io.Serializable;
import java.util.List;

import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.bson.element.TimestampElement;

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
     * Returns the value for BSON element as a Java {@link Object} type.
     * <p>
     * Automatic conversion from the Object-ified value to an element is
     * provided via the {@link DocumentBuilder#add(String, Object)} method. Not
     * all element types will be successfully converted to the same element
     * duing a Element-->Object value-->Element conversion. This cases are noted
     * in the appropriate sub-type's JavaDoc.
     * <p>
     * Sub-types will also overload this method with the appropriate type
     * returned. e.g., The
     * {@link com.allanbank.mongodb.bson.element.StringElement#getValueAsObject()}
     * method signature returns a {@link String}.
     * </p>
     * 
     * @return The value for BSON element as a Java {@link Object} type.
     */
    public Object getValueAsObject();

    /**
     * Returns the value for BSON element as a Java {@link String}. Automatic
     * conversion from the string value back to an Element is not provided.
     * <p>
     * Generally the string returned will be the expected value. As an example
     * for a LongElement with the value 101 the returned string will be "101".
     * In those cases where there is not canonical form for the value (e.g., a
     * {@link com.allanbank.mongodb.bson.element.TimestampElement} the returned
     * string will match the value when converted to JSON by the
     * {@link JsonSerializationVisitor}. For a {@link TimestampElement} that is
     * a string of the form "ISODate('1970-01-01T00:00:00.000+0000')".
     * </p>
     * 
     * @return The value for BSON element as a {@link String}.
     */
    public String getValueAsString();

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
