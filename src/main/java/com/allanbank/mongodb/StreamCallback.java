/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * Extension of the {@link Callback} interface to provide the ability to notify
 * callers when the stream is done.
 * 
 * @param <V>
 *            The type of the operations result.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface StreamCallback<V> extends Callback<V> {

    /**
     * Called when the stream of MongoDB documents has been exhausted.
     */
    public void done();
}
