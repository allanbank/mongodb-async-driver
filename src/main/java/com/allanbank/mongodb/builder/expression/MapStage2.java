/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

/**
 * MapStage2 provides a container for the {@code $map} input field name and
 * {@code as} variable name before adding the expression.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapStage2 {

    /** The name of the {@code input} field. */
    private final String myInputField;

    /** The name of the {@code as} variable. */
    private final String myVariableName;

    /**
     * Creates a new MapStage2.
     * 
     * @param inputField
     *            The name of the {@code input} field.
     * @param variableName
     *            The name of the {@code as} variable.
     */
    /* package */MapStage2(final String inputField, final String variableName) {
        myInputField = inputField;
        myVariableName = variableName;
    }

    public Expression in(final Expression mapOperation) {
        return Expressions.map(myInputField, myVariableName, mapOperation);
    }
}
