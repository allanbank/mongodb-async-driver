/*
 * #%L
 * UnaryExpression.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.builder.expression;

import java.io.StringWriter;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;

/**
 * UnaryExpression provides an implementation of an {@link Expression} with 1
 * operand.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * "$op" : expression
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
     * { &lt;name&gt; : { "$op" : expression } }
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public DocumentElement toElement(final String name) {
        return new DocumentElement(name, myExpression.toElement(myOperator));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the expression in JSON format.
     * </p>
     * <blockquote>
     * 
     * <pre>
     * <code>
     * "$op" : expression
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Override
    public String toString() {
        final StringWriter sink = new StringWriter();
        final JsonSerializationVisitor json = new JsonSerializationVisitor(
                sink, true);

        asElement().accept(json);

        return sink.toString();
    }
}