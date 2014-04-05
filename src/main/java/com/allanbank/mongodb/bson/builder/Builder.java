/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.builder;

/**
 * Common interface for all builders.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     *
     * @return This builder for method call chaining.
     */
    public Builder reset();
}
