/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Closeable;

/**
 * BatchedAsyncMongoCollection provides the interface for submitting batched
 * requests to the MongoDB server.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 * 
 * @see AsyncMongoCollection
 * @see MongoCollection
 */
public interface BatchedAsyncMongoCollection extends AsyncMongoCollection,
        Closeable {
    /**
     * Cancels the pending batch of operations without sending them to the
     * server.
     * <p>
     * After canceling the current batch you may continue to accumulate
     * additional operations to be sent in a different batch.
     * </p>
     * 
     * @throws MongoDbException
     *             If there is an error submitting the batched requests.
     */
    public void cancel() throws MongoDbException;

    /**
     * Flushes the pending batch and submits all of the pending requests to the
     * server.
     * <p>
     * This method is equivalent to {@link #flush()} and is only provided to
     * implement the {@link Closeable} interface to support try-with-resource.
     * </p>
     * 
     * @throws MongoDbException
     *             If there is an error submitting the batched requests.
     */
    @Override
    public void close() throws MongoDbException;

    /**
     * Flushes the pending batch and submits all of the pending requests to the
     * server.
     * <p>
     * After flushing the current batch you may continue to accumulate
     * additional operations to be sent in a different batch.
     * </p>
     * 
     * @throws MongoDbException
     *             If there is an error submitting the batched requests.
     */
    public void flush() throws MongoDbException;

}
