/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

/**
 * Common interface for all builders.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Builder {
    /**
     * Pops the sub-context. Since the outer context might be an array or a
     * document the appropriate builder type cannot be returned and the user
     * must maintain the appropriate outer scoped builder.
     * 
     * @return The outer scoped builder.
     */
    public Builder pop();

    /**
     * Resets the builder back to an empty state.
     */
    public void reset();
}
