/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import com.allanbank.mongodb.bson.Element;

/**
 * Represents constant value in an expression.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the constant value.
     * </p>
     * <blockquote>
     *
     * <pre>
     * <code>
     * value
     * </code>
     * </pre>
     *
     * </blockquote>
     */
    @Override
    public String toString() {
        return myConstant.getValueAsString();
    }
}
