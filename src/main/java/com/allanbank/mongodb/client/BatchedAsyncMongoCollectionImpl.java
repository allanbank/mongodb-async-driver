/*
 * Copyright 2013-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWrite.Bundle;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.client.callback.AbstractReplyCallback;
import com.allanbank.mongodb.client.callback.BatchedInsertCountingCallback;
import com.allanbank.mongodb.client.callback.BatchedWriteCallback;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;

/**
 * BatchedAsyncMongoCollectionImpl provides the implementation for the
 * {@link BatchedAsyncMongoCollection}.
 * 
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedAsyncMongoCollectionImpl extends
        AbstractAsyncMongoCollection implements BatchedAsyncMongoCollection {

    /** The interfaces to implement via the proxy. */
    private static final Class<?>[] CLIENT_INTERFACE = new Class[] { Client.class };

    /** set to true to batch deletes. */
    private boolean myBatchDeletes = false;

    /** Set to true to batch updates. */
    private boolean myBatchUpdates = false;

    /** The mode for the writes. */
    private BatchedWriteMode myMode = BatchedWriteMode.SERIALIZE_AND_CONTINUE;

    /**
     * Creates a new BatchedAsyncMongoCollectionImpl.
     * 
     * @param client
     *            The client for interacting with MongoDB.
     * @param database
     *            The database we interact with.
     * @param name
     *            The name of the collection we interact with.
     */
    public BatchedAsyncMongoCollectionImpl(final Client client,
            final MongoDatabase database, final String name) {

        super((Client) Proxy.newProxyInstance(
                BatchedAsyncMongoCollectionImpl.class.getClassLoader(),
                CLIENT_INTERFACE, new CaptureClientHandler(client)), database,
                name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to clear any pending messages without sending them to MongoDB.
     * </p>
     */
    @Override
    public void cancel() {
        final InvocationHandler handler = Proxy.getInvocationHandler(myClient);
        if (handler instanceof CaptureClientHandler) {
            ((CaptureClientHandler) handler).clear();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to flush any pending messages to a real serialized client.
     * </p>
     */
    @Override
    public void close() throws MongoDbException {
        flush();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to flush any pending messages to a real serialized client.
     * </p>
     */
    @Override
    public void flush() throws MongoDbException {
        final InvocationHandler handler = Proxy.getInvocationHandler(myClient);
        if (handler instanceof CaptureClientHandler) {
            ((CaptureClientHandler) handler).flush(this);
        }
    }

    /**
     * Returns the mode for the batched writes.
     * 
     * @return The mode for the batched writes.
     */
    public BatchedWriteMode getMode() {
        return myMode;
    }

    /**
     * Returns true if the deletes should be batched.
     * 
     * @return True if the deletes should be batched.
     */
    public boolean isBatchDeletes() {
        return myBatchDeletes;
    }

    /**
     * Returns true if the updates should be batched.
     * 
     * @return True if the updates should be batched.
     */
    public boolean isBatchUpdates() {
        return myBatchUpdates;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBatchDeletes(final boolean batchDeletes) {
        myBatchDeletes = batchDeletes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBatchUpdates(final boolean batchUpdates) {
        myBatchUpdates = batchUpdates;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMode(final BatchedWriteMode mode) {
        myMode = mode;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return false to force the {@link AbstractMongoOperations}
     * class to always use the legacy {@link Insert}, {@link Update}, and
     * {@link Delete} messages. The {@code CaptureClientHandler.optimize()} will
     * convert those operations to bulk write commands as appropriate.
     */
    @Override
    protected boolean useWriteCommand() {
        return false;
    }

    /**
     * CaptureClientHandler provides an {@link InvocationHandler} to capture all
     * send requests and defer them until flushed.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    private static class CaptureClientHandler implements InvocationHandler {

        /** The first version to support batch write commands. */
        public static final Version BATCH_WRITE_VERSION = Version
                .parse("2.5.4");

        /** The collection we are proxying. */
        private BatchedAsyncMongoCollectionImpl myCollection;

        /** The real (e.g., user's) callbacks. */
        private List<Callback<Reply>> myRealCallbacks;

        /**
         * The {@link Client} implementation to delegate to when sending
         * messages or handling other method calls.
         */
        private final Client myRealClient;

        /** The final results from the callback. */
        private List<Object> myResults;

        /**
         * The {@link Client} implementation to delegate to when sending
         * messages or handling other method calls.
         */
        private final List<Object[]> mySendArgs;

        /** The batched writer we are building. */
        private final BatchedWrite.Builder myWrite;

        /**
         * Creates a new CaptureClientHandler.
         * 
         * @param realClient
         *            The {@link Client} implementation to delegate to when
         *            sending messages or handling other method calls.
         */
        public CaptureClientHandler(final Client realClient) {
            myRealClient = realClient;

            myRealCallbacks = null;
            myResults = null;

            mySendArgs = new LinkedList<Object[]>();
            myWrite = BatchedWrite.builder();
        }

        /**
         * Clears the pending messages without sending them to MongoDB.
         */
        public synchronized void clear() {
            final List<Object[]> copy = new ArrayList<Object[]>(mySendArgs);

            mySendArgs.clear();
            myWrite.reset();

            myResults = null;
            myRealCallbacks = null;
            myCollection = null;

            for (final Object[] args : copy) {
                final Object lastArg = args[args.length - 1];
                if (lastArg instanceof Future<?>) {
                    ((Future<?>) lastArg).cancel(false);
                }
                else if (lastArg instanceof Callback<?>) {
                    ((Callback<?>) lastArg)
                            .exception(new CancellationException(
                                    "Batch request cancelled."));
                }
            }
        }

        /**
         * Flushes the pending messages to a serialized client.
         * 
         * @param collection
         *            The Collection the we are flushing operations for.
         */
        public synchronized void flush(
                final BatchedAsyncMongoCollectionImpl collection) {

            // Use a serialized client to keep all of the messages on a single
            // connection as much as possible.
            SerialClientImpl serialized;
            if (myRealClient instanceof SerialClientImpl) {
                serialized = (SerialClientImpl) myRealClient;
            }
            else {
                serialized = new SerialClientImpl((ClientImpl) myRealClient);
            }

            try {
                // Send the optimized requests.
                final List<Object> optimized = optimize(collection);
                for (final Object toSend : optimized) {
                    if (toSend instanceof BatchedWriteCallback) {
                        final BatchedWriteCallback cb = (BatchedWriteCallback) toSend;
                        cb.setClient(serialized);
                        cb.send();
                    }
                    else if (toSend instanceof Object[]) {
                        final Object[] sendArg = (Object[]) toSend;
                        if (sendArg.length == 2) {
                            serialized.send((Message) sendArg[0],
                                    (ReplyCallback) sendArg[1]);
                        }
                        else {
                            serialized.send((Message) sendArg[0],
                                    (Message) sendArg[1],
                                    (ReplyCallback) sendArg[2]);
                        }
                    }
                }
            }
            finally {
                clear();
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to batch all {@link Client#send} operations.
         * </p>
         */
        @Override
        public synchronized Object invoke(final Object proxy,
                final Method method, final Object[] args) throws Throwable {

            final String methodName = method.getName();

            if (methodName.equals("send")) {
                mySendArgs.add(args);
                return null;
            }
            return method.invoke(myRealClient, args);
        }

        /**
         * Adds a delete to the batch.
         * 
         * @param delete
         *            The delete to add to the batch.
         * @param args
         *            The raw send() arguments.
         */
        private void addDelete(final Delete delete, final Object[] args) {

            updateDurability(args);

            myRealCallbacks.add(extractCallback(args));
            myWrite.delete(delete.getQuery(), delete.isSingleDelete());
        }

        /**
         * Adds an insert to the batch.
         * 
         * @param insert
         *            The insert to add to the batch.
         * @param args
         *            The raw send() arguments.
         */
        private void addInsert(final Insert insert, final Object[] args) {

            updateDurability(args);

            final int docCount = insert.getDocuments().size();
            Callback<Reply> cb = extractCallback(args);
            final boolean breakBatch = (cb != null)
                    && insert.isContinueOnError() && (docCount > 1);

            if (breakBatch) {
                closeBatch();
                myWrite.setMode(BatchedWriteMode.SERIALIZE_AND_STOP);
            }
            else {
                cb = new BatchedInsertCountingCallback(cb, docCount);
            }

            for (final Document doc : insert.getDocuments()) {
                myWrite.insert(doc);
                myRealCallbacks.add(cb);
            }
            if (breakBatch) {
                closeBatch();
            }
        }

        /**
         * Adds an update to the batch.
         * 
         * @param update
         *            The update to add to the batch.
         * @param args
         *            The raw send() arguments.
         */
        private void addUpdate(final Update update, final Object[] args) {

            updateDurability(args);

            myRealCallbacks.add(extractCallback(args));
            myWrite.update(update.getQuery(), update.getUpdate(),
                    update.isMultiUpdate(), update.isUpsert());
        }

        /**
         * Closes the current batch of operations and re-initializes the batched
         * writer.
         */
        private void closeBatch() {
            final ClusterStats stats = myRealClient.getClusterStats();
            final BatchedWrite w = myWrite.build();
            final List<Bundle> bundles = w.toBundles(myCollection.getName(),
                    stats.getSmallestMaxBsonObjectSize(),
                    stats.getSmallestMaxBatchedWriteOperations());
            if (!bundles.isEmpty()) {
                final BatchedWriteCallback cb = new BatchedWriteCallback(
                        myCollection.getDatabaseName(), myCollection.getName(),
                        myRealCallbacks, w, bundles);
                myResults.add(cb);
            }

            myWrite.reset();
            myWrite.setMode(myCollection.getMode());

            myRealCallbacks.clear();
        }

        /**
         * Extracts the callback from the write arguments. If the write has a
         * {@link Callback} then it will be the last argument.
         * 
         * @param args
         *            The arguments for the original {@link Client#send} call.
         * @return The callback for the call. Returns null if there is no
         *         {@link Callback}.
         */
        private Callback<Reply> extractCallback(final Object[] args) {
            final Object cb = args[args.length - 1];
            if (cb instanceof AbstractReplyCallback<?>) {
                return (AbstractReplyCallback<?>) args[2];
            }

            return null;
        }

        /**
         * Tries the optimize the messages we will send to the server by
         * coalescing the sequential insert, update and delete messages into the
         * batched write commands of the same name.
         * 
         * @param collection
         *            The collection we are sending requests to.
         * @return The list of optimized messages.
         */
        private List<Object> optimize(
                final BatchedAsyncMongoCollectionImpl collection) {

            if (mySendArgs.isEmpty()) {
                return Collections.emptyList();
            }

            final ClusterStats stats = myRealClient.getClusterStats();
            final Version minVersion = stats.getServerVersionRange()
                    .getLowerBounds();
            final boolean supportsBatch = BATCH_WRITE_VERSION
                    .compareTo(minVersion) <= 0;
            if (supportsBatch) {
                myCollection = collection;

                myWrite.reset();
                myWrite.setMode(collection.getMode());

                myResults = new ArrayList<Object>(mySendArgs.size());
                myRealCallbacks = new ArrayList<Callback<Reply>>(
                        mySendArgs.size());

                while (!mySendArgs.isEmpty()) {
                    final Object[] args = mySendArgs.remove(0);
                    if (args[0] instanceof Insert) {
                        addInsert((Insert) args[0], args);
                    }
                    else if (collection.isBatchUpdates()
                            && (args[0] instanceof Update)) {
                        addUpdate((Update) args[0], args);
                    }
                    else if (collection.isBatchDeletes()
                            && (args[0] instanceof Delete)) {
                        addDelete((Delete) args[0], args);
                    }
                    else {
                        closeBatch();
                        myResults.add(args);
                    }

                    if (collection.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP) {
                        closeBatch();
                    }
                }

                closeBatch();
            }
            else {
                myResults = new ArrayList<Object>(mySendArgs.size());
                myResults.addAll(mySendArgs);

                // Clear the sendArgs or they will get notified of a cancel.
                mySendArgs.clear();
            }

            return myResults;
        }

        /**
         * Updates the durability for the batch. If the durability changes
         * mid-batch then we force a break in the batch.
         * 
         * @param args
         *            The arguments for the send() call. The
         *            {@link GetLastError} will be the second of three
         *            arguments.
         */
        private void updateDurability(final Object[] args) {

            Durability active = myWrite.getDurability();

            if ((args.length == 3) && (args[1] instanceof GetLastError)) {
                final GetLastError error = (GetLastError) args[1];

                final Durability d = Durability.valueOf(error.getQuery()
                        .toString());

                if (active == null) {
                    active = d;
                    myWrite.setDurability(active);
                }
                else if (!d.equals(active) && !d.equals(Durability.ACK)
                        && !d.equals(Durability.NONE)) {
                    closeBatch();
                    active = d;
                    myWrite.setDurability(active);
                }
            } // else Durability is none or not applicable.
        }
    }
}
