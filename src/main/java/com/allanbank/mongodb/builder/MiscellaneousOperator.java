/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * MiscellaneousOperator provides the set of miscellaneous operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum MiscellaneousOperator implements Operator {
    /**
     * Operator to ensure that all values provided in the operand existing in
     * the array field.
     */
    ALL("$all"),

    /** Provides the ability to match an entire array element at once. */
    ELEMENT_MATCH("$elemMatch"),

    /** Check to see if a field exists or not. */
    EXISTS("$exists"),

    /** Checks if a value for a field is present in the operand array. */
    IN("$in"),

    /**
     * Performs a modulo operation on a field and is equivalent to the where
     * statement <code>field % d == m</code>.
     */
    MOD("$mod"),

    /** Checks if a value for a field is not present in the operand array. */
    NIN("$nin"),

    /** Test if a field matches a specified regular expression. */
    REG_EX("$regex"),

    /** Compares the length of the array field. */
    SIZE("$size"),

    /** Check if a value's type matches the expected type. */
    TYPE("$type"),

    /** Support for an ad-hoc JavaScript expression. */
    WHERE("$where");

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /**
     * Creates a new MiscellaneousOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private MiscellaneousOperator(final String token) {
        myToken = token;
    }

    /**
     * The token for the operator that can be sent to the server.
     * 
     * @return The token for the operator.
     */
    @Override
    public String getToken() {
        return myToken;
    }

}
