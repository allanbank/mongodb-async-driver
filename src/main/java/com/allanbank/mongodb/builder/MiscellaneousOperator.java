/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.Version;

/**
 * MiscellaneousOperator provides the set of miscellaneous operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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

    /**
     * Support for text searches.
     * 
     * @since MongoDB 2.6
     */
    TEXT("$text", Version.VERSION_2_6),

    /** Check if a value's type matches the expected type. */
    TYPE("$type"),

    /** Support for an ad-hoc JavaScript expression. */
    WHERE("$where");

    /**
     * The modifier for the {@link #TEXT} operator to specify the language of
     * the query terms.
     * 
     * @since MongoDB 2.6
     */
    public static final String LANGUAGE_MODIFIER = "$language";

    /**
     * The modifier for the {@link #TEXT} operator to specify the the query
     * terms.
     * 
     * @since MongoDB 2.6
     */
    public static final String SEARCH_MODIFIER = "$search";

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /** The first MongoDB version to support the operator. */
    private final Version myVersion;

    /**
     * Creates a new MiscellaneousOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private MiscellaneousOperator(final String token) {
        this(token, null);
    }

    /**
     * Creates a new MiscellaneousOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     * @param version
     *            The first MongoDB version to support the operator.
     */
    private MiscellaneousOperator(final String token, final Version version) {
        myToken = token;
        myVersion = version;
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

    /**
     * Returns the first MongoDB version to support the operator.
     * 
     * @return The first MongoDB version to support the operator. If
     *         <code>null</code> then the version is not known and can be
     *         assumed to be all currently supported versions.
     */
    @Override
    public Version getVersion() {
        return myVersion;
    }

}
