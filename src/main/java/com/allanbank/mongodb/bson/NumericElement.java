/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

/**
 * NumericElement provides a common interface for all numeric {@link Element}s.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
