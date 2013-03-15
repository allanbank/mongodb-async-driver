/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * NumericElement provides a common interface for all numeric {@link Element}s.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface NumericElement extends Element {

    /**
     * Returns the value cast to a double.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.DoubleElement}. </em>
     * 
     * @return The numeric value as a double.
     */
    public double getDoubleValue();

    /**
     * Returns the value cast to an integer.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.IntegerElement}. </em>
     * 
     * @return The numeric value as a double.
     */
    public int getIntValue();

    /**
     * Returns the value cast to a long.
     * <p>
     * <em>Note</em>: There may be a loss of precision using this method if the
     * {@link NumericElement} is not a
     * {@link com.allanbank.mongodb.bson.element.LongElement}. </em>
     * 
     * @return The numeric value as a double.
     */
    public long getLongValue();
}
