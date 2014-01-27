/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;

/**
 * BatchedAsyncMongoCollectionImpl provides the implementation for the
 * {@link BatchedAsyncMongoCollection}.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedAsyncMongoCollectionImpl extends
        AbstractAsyncMongoCollection implements BatchedAsyncMongoCollection {

    /** The interfaces to implement via the proxy. */
    private static final Class<?>[] CLIENT_INTERFACE = new Class[] { Client.class };

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
     * Overridden to flush any pending messages to a real serialized client.
     * </p>
     */
    @Override
    public void close() throws MongoDbException {
        final InvocationHandler handler = Proxy.getInvocationHandler(myClient);
        if (handler instanceof CaptureClientHandler) {
            ((CaptureClientHandler) handler).flush();
        }
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

        /**
         * The {@link Client} implementation to delegate to when sending
         * messages or handling other method calls.
         */
        private final Client myRealClient;

        /**
         * The {@link Client} implementation to delegate to when sending
         * messages or handling other method calls.
         */
        private final List<Object[]> mySendArgs;

        /**
         * Creates a new CaptureClientHandler.
         * 
         * @param realClient
         *            The {@link Client} implementation to delegate to when
         *            sending messages or handling other method calls.
         */
        public CaptureClientHandler(final Client realClient) {
            myRealClient = realClient;
            mySendArgs = new LinkedList<Object[]>();
        }

        /**
         * Flushes the pending messages to a serialized client.
         */
        @SuppressWarnings("unchecked")
        public void flush() {

            // Use a serialized client to keep all of the messages on a single
            // connection as much as possible.
            SerialClientImpl serialized;
            if (myRealClient instanceof SerialClientImpl) {
                serialized = (SerialClientImpl) myRealClient;
            }
            else {
                serialized = new SerialClientImpl((ClientImpl) myRealClient);
            }

            // Send the optimized requests.
            final List<Object[]> optimized = optimize();
            for (final Object[] sendArg : optimized) {
                if (sendArg.length == 2) {
                    serialized.send((Message) sendArg[0],
                            (Callback<Reply>) sendArg[1]);
                }
                else {
                    serialized.send((Message) sendArg[0], (Message) sendArg[1],
                            (Callback<Reply>) sendArg[2]);
                }
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overriden to TODO Finish.
         * </p>
         */
        @Override
        public Object invoke(final Object proxy, final Method method,
                final Object[] args) throws Throwable {

            final String methodName = method.getName();

            if (methodName.equals("send")) {
                mySendArgs.add(args);
                return null;
            }
            return method.invoke(myRealClient, args);
        }

        /**
         * Tries the optimize the messages we will send to the server by
         * coalescing the sequential insert, update and delete messages into the
         * batched write commands of the same name.
         * 
         * @return The list of optimized messages.
         */
        private List<Object[]> optimize() {

            final Version version = myRealClient.getMinimumServerVersion();
            final boolean supportsBatch = BATCH_WRITE_VERSION
                    .compareTo(version) <= 0;

            final List<Object[]> results = new ArrayList<Object[]>(
                    mySendArgs.size());
            while (!mySendArgs.isEmpty()) {
                final Object[] args = mySendArgs.remove(0);
                if (supportsBatch && (args[0] instanceof Insert)) {
                    // TODO: Implement the batch insert command.
                    results.add(args);
                    while (!mySendArgs.isEmpty()
                            && (mySendArgs.get(0)[0] instanceof Insert)) {
                        final Object[] nextInsert = mySendArgs.remove(0);

                        results.add(nextInsert);
                    }
                }
                else if (supportsBatch && (args[0] instanceof Update)) {
                    // TODO: Implement the batch insert command.
                    results.add(args);
                    while (!mySendArgs.isEmpty()
                            && (mySendArgs.get(0)[0] instanceof Update)) {
                        final Object[] nextUpdate = mySendArgs.remove(0);

                        results.add(nextUpdate);
                    }
                }
                else if (supportsBatch && (args[0] instanceof Delete)) {
                    // TODO: Implement the batch insert command.
                    results.add(args);
                    while (!mySendArgs.isEmpty()
                            && (mySendArgs.get(0)[0] instanceof Delete)) {
                        final Object[] nextDelete = mySendArgs.remove(0);

                        results.add(nextDelete);
                    }
                }
                else {
                    results.add(args);
                }
            }

            return results;
        }
    }
}
