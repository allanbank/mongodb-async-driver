/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.write;

/**
 * WriteOperation provides a common interface for all types of writes:
 * {@link InsertOperation inserts}, {@link UpdateOperation updates}, and
 * {@link DeleteOperation deletes}.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface WriteOperation {
    /**
     * Returns the type of write. Can be used to avoid instance of calls and in
     * switch statements.
     * 
     * @return The type of write.
     */
    public WriteOperationType getType();
}