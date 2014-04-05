/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.builder.write.DeleteOperation;
import com.allanbank.mongodb.builder.write.InsertOperation;
import com.allanbank.mongodb.builder.write.UpdateOperation;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.AbstractMongoOperations;
import com.allanbank.mongodb.error.BatchedWriteException;

/**
 * BatchedWriteCallback provides the global callback for the batched writes when
 * the server does not support the write commands. This callback will issue the
 * writes using the original wire protocol.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedNativeWriteCallback extends ReplyLongCallback {

    /**
     * NativeCallback provides the callback for a single operation within a
     * batched write. This callback is used when the batched write commands are
     * not supported and the driver falls back to using the native insert,
     * update, and delete messages.
     *
     * @param <T>
     *            The type for the callback. Expected to be either Integer or
     *            Long.
     *
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    /* package */class NativeCallback<T extends Number> implements Callback<T> {

        /** The operation this callback is waiting for the reply from. */
        private final WriteOperation myOperation;

        /**
         * Creates a new BatchedWriteNativeCallback.
         *
         * @param operation
         *            The operation this callback is waiting for the reply from.
         */
        public NativeCallback(final WriteOperation operation) {
            myOperation = operation;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward the results to the parent callback.
         * </p>
         */
        @Override
        public void callback(final T result) {
            BatchedNativeWriteCallback.this.callback(myOperation,
                    result.longValue());
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward the error to the parent callback.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            BatchedNativeWriteCallback.this.exception(myOperation, thrown);
        }
    }

    /** The collection to send individual operations with. */
    private final AbstractMongoOperations myCollection;

    /** The list of write operations which failed. */
    private final Map<WriteOperation, Throwable> myFailedOperations;

    /** The count of finished bundles or operations. */
    private int myFinished;

    /** The result. */
    private long myN = 0;

    /** The list of write operations to send. */
    private final List<WriteOperation> myOperations;

    /** The list of write operations waiting to be sent to the server. */
    private final List<WriteOperation> myPendingOperations;

    /** The original write operation. */
    private final BatchedWrite myWrite;

    /**
     * Creates a new BatchedWriteCallback.
     *
     * @param results
     *            The callback for the final results.
     * @param write
     *            The original write.
     * @param collection
     *            The collection for sending the operations.
     * @param operations
     *            The operations to send.
     */
    public BatchedNativeWriteCallback(final Callback<Long> results,
            final BatchedWrite write, final AbstractMongoOperations collection,
            final List<WriteOperation> operations) {
        super(results);

        myWrite = write;
        myCollection = collection;
        myOperations = Collections
                .unmodifiableList(new ArrayList<WriteOperation>(operations));

        myPendingOperations = new LinkedList<WriteOperation>(myOperations);

        myFinished = 0;
        myN = 0;

        myFailedOperations = new IdentityHashMap<WriteOperation, Throwable>();
    }

    /**
     * Sends the next set of operations to the server.
     */
    public void send() {

        List<WriteOperation> toSendOperations = Collections.emptyList();
        Durability durability = null;

        synchronized (this) {
            List<WriteOperation> toSend = myPendingOperations;
            if (BatchedWriteMode.SERIALIZE_AND_STOP.equals(myWrite.getMode())) {
                toSend = myPendingOperations.subList(0, 1);
            }

            durability = myWrite.getDurability();
            if ((durability == null) || (durability == Durability.NONE)) {
                durability = Durability.ACK;
            }

            // Clear toSend before sending so the callbacks see the right
            // state for the operations.
            toSendOperations = new ArrayList<WriteOperation>(toSend);
            toSend.clear();
        } // Release lock.

        // Release the lock before sending to avoid deadlock in processing
        // replies.

        for (final WriteOperation operation : toSendOperations) {
            switch (operation.getType()) {
            case INSERT: {
                final InsertOperation insert = (InsertOperation) operation;
                myCollection.insertAsync(new NativeCallback<Integer>(insert),
                        true, durability, insert.getDocument());
                break;
            }
            case UPDATE: {
                final UpdateOperation update = (UpdateOperation) operation;
                myCollection.updateAsync(new NativeCallback<Long>(operation),
                        update.getQuery(), update.getUpdate(),
                        update.isMultiUpdate(), update.isUpsert(), durability);
                break;
            }
            case DELETE: {
                final DeleteOperation delete = (DeleteOperation) operation;
                myCollection.deleteAsync(new NativeCallback<Long>(operation),
                        delete.getQuery(), delete.isSingleDelete(), durability);
                break;
            }
            }
        }
    }

    /**
     * Callback for a single write operation sent via the native messages.
     *
     * @param operation
     *            The write operation.
     * @param result
     *            The result of the write operation.
     */
    protected synchronized void callback(final WriteOperation operation,
            final long result) {
        myN += result;
        myFinished += 1;

        if (!myPendingOperations.isEmpty()) {
            send();
        }
        else if (myFinished == myOperations.size()) {
            publishResults();
        }
    }

    /**
     * Callback for a single write operation sent via the native messages has
     * failed.
     *
     * @param operation
     *            The write operation.
     * @param thrown
     *            The error for the operation.
     */
    protected synchronized void exception(final WriteOperation operation,
            final Throwable thrown) {
        myFinished += 1;
        myFailedOperations.put(operation, thrown);

        if (myWrite.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP) {
            publishResults();
        }
        // No need to check if we have to send. Would have already sent all of
        // the operations if not SERIALIZE_AND_STOP.
        else if (myFinished == myOperations.size()) {
            publishResults();
        }

    }

    /**
     * Publishes the final results.
     */
    private void publishResults() {
        if (myFailedOperations.isEmpty()) {
            myForwardCallback.callback(Long.valueOf(myN));
        }
        else {
            myForwardCallback.exception(new BatchedWriteException(myWrite, myN,
                    myPendingOperations, myFailedOperations));
        }
    }
}
