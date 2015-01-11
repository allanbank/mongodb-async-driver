/*
 * #%L
 * BatchedAsyncMongoCollection.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb;

import java.io.Closeable;

import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.builder.BatchedWriteMode;

/**
 * BatchedAsyncMongoCollection provides the interface for submitting batched
 * requests to the MongoDB server. The behavior of the batching is different
 * based on the lowest version member of the MongoDB cluster.
 *
 * <h4>Pre MongoDB 2.6</h4>
 * <p>
 * For MongoDB servers prior to 2.6 each command is sent to the server exactly
 * as it would have been using the non-batched interface except that the
 * commands are sent using a single logical connection. Queries using different,
 * non-primary, {@link ReadPreference}s may use different physical connections.
 * </p>
 *
 * <h4>Post MongoDB 2.6</h4>
 * <p>
 * With the introduction of commands to batch inserts, updates and deletes we
 * can now group each sequence of inserts, updates, and deletes within the
 * batch. The actual logic for what writes can be grouped together is controlled
 * by the {@link #setMode(BatchedWriteMode) mode}. Users should be sure that
 * they have read and understand the {@link #setMode(BatchedWriteMode)} JavaDoc.
 * Similar to the pre-2.6 case all of the operations are sent using a single
 * logical connection. Queries using different, non-primary,
 * {@link ReadPreference}s may use different physical connections.
 * </p>
 * <p>
 * <b>Warning</b>: In the case of
 * {@link BatchedWriteMode#SERIALIZE_AND_CONTINUE} and
 * {@link BatchedWriteMode#REORDERED} it is impossible to determine the number
 * of documents each update and/or delete touched. In these cases each update in
 * the batch will be returned the number of documents touched by the batch as a
 * whole. For this reason the batching of updates and deletes is disabled. It
 * can be enabled by setting {@link #setBatchUpdates(boolean)} and
 * {@link #setBatchDeletes(boolean)} to true. We have written a bug report (<a
 * href="https://jira.mongodb.org/browse/SERVER-12858">SERVER-12858</a>) to have
 * the additional information added to the responses so we can remove this
 * restriction.
 * </p>
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
@ThreadSafe
public interface BatchedAsyncMongoCollection
        extends AsyncMongoCollection, Closeable {
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

    /**
     * Sets if deletes should be batched. Set to true to batch deletes.
     * <p>
     * Defaults to false since there is no way to determine how many documents
     * each deletes removed when they are batched.
     * </p>
     * <p>
     * This setting takes effect for a batch when it is {@link #close() closed}
     * or {@link #flush() flushed}. Intermediate changes between flushes have no
     * effect.
     * </p>
     *
     * @param batchDeletes
     *            Set to true to batch deletes.
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-12858">SERVER-12858</a>
     */
    public void setBatchDeletes(boolean batchDeletes);

    /**
     * Sets if updates should be batched. Set to true to batch updates.
     * <p>
     * Defaults to false since there is no way to determine how many documents
     * each update touched when they are batched.
     * </p>
     * <p>
     * This setting takes effect for a batch when it is {@link #close() closed}
     * or {@link #flush() flushed}. Intermediate changes between flushes have no
     * effect.
     * </p>
     *
     * @param batchUpdates
     *            Set to true to batch updates.
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-12858">SERVER-12858</a>
     */
    public void setBatchUpdates(boolean batchUpdates);

    /**
     * Sets the default mode for batching of writes. This is only applicable to
     * situations where the server supports the MongoDB write command (i.e.,
     * version 2.6 and later). There are two cases where the mode will be
     * ignored:
     * <ul>
     * <li>
     * All inserts with continueOnError set to true will be executed as distinct
     * batches.</li>
     * <li>
     * Any change in the durability within the sequence of writes will case the
     * writes to be broken across batches sent to the server. This does not
     * apply to {@link Durability#ACK} and {@link Durability#NONE} writes and
     * they may be grouped with other writes. Users should be aware that this
     * may cause unexpected write concern failures in those cases.</li>
     * </ul>
     * <p>
     * The default mode is {@link BatchedWriteMode#SERIALIZE_AND_CONTINUE}. This
     * provides reasonable performance while maintaining the expected semantics.
     * Note that this mode has the effect of coalescing sequences of inserts
     * (where continueOnError is true), updates and deletes into single
     * operations.
     * </p>
     * <p>
     * The {@link BatchedWriteMode#SERIALIZE_AND_STOP} will cause each insert,
     * update, and delete to be sent as independent commands.
     * </p>
     * <p>
     * The {@link BatchedWriteMode#REORDERED} will cause each sequence of
     * inserts, updates, and deletes without any other command or query to be
     * groups together, reordered, and executed together. This includes the
     * reordering writes within a type (one insert before another to better
     * packet the messages) as well as reordering the writes across types (a
     * delete before an insert to build larger batches).
     * </p>
     * <p>
     * The mode setting takes effect for a batch when it is {@link #close()
     * closed} or {@link #flush() flushed}. Intermediate changes between flushes
     * have no effect.
     * </p>
     * <p>
     * <b>Warning</b>: In the case of
     * {@link BatchedWriteMode#SERIALIZE_AND_CONTINUE} and
     * {@link BatchedWriteMode#REORDERED} it is impossible to determine the
     * number of documents each update and/or delete touched. In these cases
     * each update in the batch will be returned the number of documents touched
     * by the batch as a whole. For this reason the batching of updates and
     * deletes is disabled. It can be enabled by setting
     * {@link #setBatchUpdates(boolean)} and {@link #setBatchDeletes(boolean)}
     * to true. We have written a bug report (<a
     * href="https://jira.mongodb.org/browse/SERVER-12858">SERVER-12858</a>) to
     * have the additional information added to the responses so we can remove
     * this restriction.
     * </p>
     *
     * @param mode
     *            The default mode for
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-12858">SERVER-12858</a>
     */
    public void setMode(BatchedWriteMode mode);
}
