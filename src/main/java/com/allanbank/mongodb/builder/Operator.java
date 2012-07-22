/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * Operator provides an enumeration of all possible operators.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Operator {

    /**
     * The token for the operator that can be sent to the server.
     * 
     * @return The token for the operator.
     */
    public String getToken();
}
