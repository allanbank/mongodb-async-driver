/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * RedactOption provides options for what to do when evaluating a
 * {@code $redact} condition within the {@link Aggregate} pipeline.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum RedactOption {
    /**
     * Option for the redact to descend into the sub-documents of the current
     * document.
     */
    DESCEND("$$DESCEND"),

    /** Option to keep the current document and not inspect the sub-documents. */
    KEEP("$$KEEP"),

    /** Option to prune the current document. */
    PRUNE("$$PRUNE");

    /** The token for the option. */
    private final String myToken;

    /**
     * Creates a new RedactOption.
     *
     * @param token
     *            The token for the option.
     */
    private RedactOption(final String token) {
        myToken = token;
    }

    /**
     * Returns the token for the option.
     *
     * @return The token for the option.
     */
    public String getToken() {
        return myToken;
    }
}
