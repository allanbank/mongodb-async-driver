/*
 * #%L
 * LetBuilder.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;

/**
 * LetBuilder provides a builder for a {@code $let} expression.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class LetBuilder {
    /** The fields in the {@code $let} expression. */
    private final List<Element> myFields;

    /**
     * Creates a new MapStage2.
     *
     * @param firstField
     *            The name of the {@code input} field.
     */
    /* package */LetBuilder(final Element firstField) {
        myFields = new ArrayList<Element>();
        myFields.add(firstField);
    }

    /**
     * Creates the final {@code $let} expression to evaluate.
     *
     * @param letExpression
     *            The expression to be evaluated with the variables within the
     *            {@code $let} expression.
     * @return The {@link UnaryExpression} for the {@code $let}.
     */
    public UnaryExpression in(final Expression letExpression) {
        return Expressions.let(myFields, letExpression);
    }

    /**
     * Adds a variable to the {@code $let} expression.
     *
     * @param name
     *            The name of the variable to set.
     * @param document
     *            The document to set the variable to.
     * @return This builder for method call chaining.
     */
    public LetBuilder let(final String name, final DocumentAssignable document) {
        myFields.add(new DocumentElement(name, document.asDocument()));
        return this;
    }

    /**
     * Adds a variable to the {@code $let} expression.
     *
     * @param name
     *            The name of the variable to set.
     * @param expression
     *            The expression to compute the value for the variable.
     * @return This builder for method call chaining.
     */
    public LetBuilder let(final String name, final Expression expression) {
        myFields.add(expression.toElement(name));
        return this;
    }
}
