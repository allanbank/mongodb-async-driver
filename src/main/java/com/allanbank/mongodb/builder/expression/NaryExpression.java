/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.builder.expression;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;

/**
 * NaryExpression provides an implementation of an {@link Expression} with 2-N
 * operands.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NaryExpression implements Expression, ElementAssignable {

    /** The expression expressed as an {@link ArrayElement}. */
    protected final ArrayElement myExpressions;

    /**
     * Creates a new NaryExpression.
     * 
     * @param operator
     *            The operator this object represents.
     * @param expressions
     *            The sub expressions.
     */
    public NaryExpression(final String operator,
            final Expression... expressions) {
        final List<Element> elements = new ArrayList<Element>(
                expressions.length);
        for (int i = 0; i < expressions.length; ++i) {
            elements.add(expressions[i].toElement(String.valueOf(i)));
        }

        myExpressions = new ArrayElement(operator, elements);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the sub expressions as a {@link ArrayElement}:
     * </p>
     * <blockquote>
     * 
     * <pre>
     * <code>
     * { "$op" : [ &lt;e1&gt;, &lt;e2&gt;, &lt;e2&gt;, ... ] }
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public ArrayElement asElement() {
        return myExpressions;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the sub expressions as a document with a nested
     * array element:
     * </p>
     * <blockquote>
     * 
     * <pre>
     * <code>
     * { &lt;name&gt; : { "$op" : [ &lt;e1&gt;, &lt;e2&gt;, &lt;e2&gt;, ... ] } }
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public DocumentElement toElement(final String name) {
        return new DocumentElement(name, myExpressions);
    }

}