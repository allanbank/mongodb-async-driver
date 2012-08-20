/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.builder.expression;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.DocumentElement;

/**
 * UnaryExpression provides an implementation of an {@link Expression} with 1
 * operand.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnaryExpression implements Expression, ElementAssignable {

    /** The sub expression. */
    protected final Expression myExpression;

    /** The operator. */
    protected final String myOperator;

    /**
     * Creates a new NaryExpression.
     * 
     * @param operator
     *            The operator this object represents.
     * @param expression
     *            The sub expression.
     */
    public UnaryExpression(final String operator, final Expression expression) {
        myOperator = operator;
        myExpression = expression;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the sub expressions as an element:
     * </p>
     * <blockquote>
     * 
     * <pre>
     * <code>
     * { "$op" : ??? } 
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public Element asElement() {
        return myExpression.toElement(myOperator);
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
     * { &lt;name&gt; : { "$op" : ??? } }
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public DocumentElement toElement(final String name) {
        return new DocumentElement(name, myExpression.toElement(myOperator));
    }

}