/*
 * #%L
 * NaryExpression.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementAssignable;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;

/**
 * NaryExpression provides an implementation of an {@link Expression} with 2-N
 * operands.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class NaryExpression
        implements Expression, ElementAssignable {

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
     * "$op" : [ &lt;e1&gt;, &lt;e2&gt;, &lt;e2&gt;, ... ]
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the expression in JSON format.
     * </p>
     * <blockquote>
     *
     * <pre>
     * <code>
     * "$op" : [ &lt;e1&gt;, &lt;e2&gt;, &lt;e2&gt;, ... ]
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

        myExpressions.accept(json);

        return sink.toString();
    }
}