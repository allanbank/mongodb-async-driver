/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * Interface for a callback with the result of a MongoDB operation.
 * 
 * @param <V>
 *            The type of the operations result.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Callback<V> {

    /**
     * Called when the MongoDB operation has completed with the result of the
     * operation.
     * 
     * @param result
     *            The result of the operation.
     */
    public void callback(V result);

    /**
     * Called when the operation fails due to an exception.
     * 
     * @param thrown
     *            The thrown exception.
     */
    public void exception(Throwable thrown);

}
