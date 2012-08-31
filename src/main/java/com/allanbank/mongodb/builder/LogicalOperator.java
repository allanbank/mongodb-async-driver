/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * LogicalOperator provides the set of logical operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum LogicalOperator implements Operator {

    /** The logical conjunction operator. */
    AND("$and"),

    /** The logical negated disjunction operator. */
    NOR("$nor"),

    /** The logical negation operator. */
    NOT("$not"),

    /** The logical disjunction operator. */
    OR("$or");

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /**
     * Creates a new LogicalOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private LogicalOperator(final String token) {
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
