/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

/**
 * MapStage1 provides a container for the {@code $map} input field name before
 * adding the variable name.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapStage1 {
    /** The name of the {@code input} field. */
    private final String myInputField;

    /**
     * Creates a new MapStage2.
     * 
     * @param inputField
     *            The name of the {@code input} field.
     */
    /* package */MapStage1(final String inputField) {
        myInputField = inputField;
    }

    /**
     * Sets the variable name to be used in the mapping.
     * 
     * @param variableName
     *            The name of the variable.
     * @return The second stage of the {@code $map} construction.
     */
    public MapStage2 as(final String variableName) {
        return new MapStage2(myInputField, variableName);
    }
}
