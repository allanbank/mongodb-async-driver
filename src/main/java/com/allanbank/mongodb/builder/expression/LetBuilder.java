/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import java.util.ArrayList;
import java.util.List;

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
public class LetBuilder {
    /** The fields in the {@code $let} expression. */
    private final List<Element> myFields;

    /**
     * Creates a new MapStage2.
     * 
     * @param inputField
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
