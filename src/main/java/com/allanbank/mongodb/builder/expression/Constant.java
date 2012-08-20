/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import com.allanbank.mongodb.bson.Element;

/**
 * Represents constant value in an expression.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Constant implements Expression {

    /** The constant value. */
    private final Element myConstant;

    /**
     * Creates a new Constant.
     * 
     * @param constant
     *            The constant element.
     */
    public Constant(final Element constant) {
        myConstant = constant;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the constant value's element with the specified
     * name.
     * </p>
     */
    @Override
    public Element toElement(final String name) {
        return myConstant.withName(name);
    }
}
