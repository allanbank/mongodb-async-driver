/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * ComparisonOperator provides an enumeration of all possible comparison
 * operators.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ComparisonOperator implements Operator {
    /** The equal operator. Note that this operator does not have a token. */
    EQUALS(""),

    /** The greater than operator. */
    GT("$gt"),

    /** The greater than or equal operator. */
    GTE("$gte"),

    /** The less than operator. */
    LT("$lt"),

    /** The less than or equal operator. */
    LTE("$lte"),

    /** The not equal operator. */
    NE("$ne");

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /**
     * Creates a new ComparisonOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private ComparisonOperator(final String token) {
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
