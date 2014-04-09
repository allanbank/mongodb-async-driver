/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;

/**
 * Expression provides a common interface for all expressions. Most expression
 * classes also implement the {@link ElementAssignable}, the notable exception
 * being {@link Constant}.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Expression {
    /**
     * Returns the expression in the form of a BSON element.
     * 
     * @param name
     *            The name for the expression's element.
     * @return The expression in the form of a BSON element.
     */
    public Element toElement(String name);
}
