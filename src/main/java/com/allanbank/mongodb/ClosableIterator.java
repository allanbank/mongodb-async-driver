/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.Iterator;

/**
 * ClosableIterator provides an interface for an iterator that can be closed.
 * <p>
 * In addition the batch size for the next request for documents from the cursor
 * can be set.
 * </p>
 * 
 * @param <T>
 *            The type of elements being iterated over.
 * 
 * @deprecated Use the {@link MongoIterator} interface instead. This interface
 *             will be removed after the 1.3.0 release.
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public interface ClosableIterator<T> extends Iterator<T>, Iterable<T> {

    /**
     * Close the iterator and release any resources it is holding.
     */
    public void close();

    /**
     * Returns the size for batches of documents that are requested.
     * 
     * @return The size of the batches of documents that are requested.
     */
    public int getBatchSize();

    /**
     * Sets the size for future batch sizes.
     * 
     * @param batchSize
     *            The size to request for future batch sizes.
     */
    public void setBatchSize(int batchSize);
}
