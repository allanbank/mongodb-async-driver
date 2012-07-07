/*
 * Copyright 2012, Allanbank Consulting, Inc. 
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
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
