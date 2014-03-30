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
import java.util.Set;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWrite.Bundle;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.builder.write.DeleteOperation;
import com.allanbank.mongodb.builder.write.InsertOperation;
import com.allanbank.mongodb.builder.write.UpdateOperation;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.AbstractMongoOperations;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.BatchedWriteException;
import com.allanbank.mongodb.util.Assertions;

/**
 * BatchedWriteCallback provides the global callback for the batched writes.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteCallback extends ReplyLongCallback {

    /** A no-op callback. */
    private static final NoOpCallback NO_OP = new NoOpCallback();

    /** The list of bundles to send. */
    private final List<BatchedWrite.Bundle> myBundles;

    /** The client to send messages with. */
    private Client myClient;

    /** The collection to send individual operations with. */
    private final AbstractMongoOperations myCollection;

    /** The name of the database. */
    private final String myDatabaseName;

    /** The list of write operations which failed. */
    private final Map<WriteOperation, Throwable> myFailedOperations;

    /** The count of finished bundles or operations. */
    private int myFinished;

    /** The result. */
    private long myN = 0;

    /** The list of write operations to send. */
    private final List<WriteOperation> myOperations;

    /** The list of bundles waiting to be sent to the server. */
    private final List<BatchedWrite.Bundle> myPendingBundles;

    /** The list of write operations waiting to be sent to the server. */
    private final List<WriteOperation> myPendingOperations;

    /** The real callback for each operation. */
    private final List<Callback<Reply>> myRealCallbacks;

    /** The list of write operations which have been skipped due to an error. */
    private List<WriteOperation> mySkippedOperations;

    /** The original write operation. */
    private final BatchedWrite myWrite;

    /**
     * Creates a new BatchedWriteCallback.
     * 
     * @param databaseName
     *            The name of the database.
     * @param results
     *            The callback for the final results.
     * @param write
     *            The original write.
     * @param collection
     *            The collection for sending the operations.
     * @param operations
     *            The operations to send.
     */
    public BatchedWriteCallback(final String databaseName,
            final Callback<Long> results, final BatchedWrite write,
            final AbstractMongoOperations collection,
            final List<WriteOperation> operations) {
        super(results);

        myDatabaseName = databaseName;
        myWrite = write;
        myClient = null;
        myBundles = Collections.emptyList();
        myCollection = collection;
        myOperations = Collections
                .unmodifiableList(new ArrayList<WriteOperation>(operations));

        myPendingBundles = Collections.emptyList();
        myPendingOperations = new LinkedList<WriteOperation>(myOperations);

        myFinished = 0;
        myFinished = 0;

        myFailedOperations = new IdentityHashMap<WriteOperation, Throwable>();
        mySkippedOperations = null;

        myRealCallbacks = Collections.emptyList();
    }

    /**
     * Creates a new BatchedWriteCallback.
     * 
     * @param databaseName
     *            The name of the database.
     * @param results
     *            The callback for the final results.
     * @param write
     *            The original write.
     * @param client
     *            The client for sending the bundled write commands.
     * @param bundles
     *            The bundled writes.
     */
    public BatchedWriteCallback(final String databaseName,
            final Callback<Long> results, final BatchedWrite write,
            final Client client, final List<BatchedWrite.Bundle> bundles) {
        super(results);

        myDatabaseName = databaseName;
        myWrite = write;
        myClient = client;
        myBundles = Collections
                .unmodifiableList(new ArrayList<BatchedWrite.Bundle>(bundles));
        myCollection = null;
        myOperations = Collections.emptyList();

        myPendingBundles = new LinkedList<BatchedWrite.Bundle>(myBundles);
        myPendingOperations = Collections.emptyList();

        myFinished = 0;
        myFinished = 0;

        myFailedOperations = new IdentityHashMap<WriteOperation, Throwable>();
        mySkippedOperations = null;

        myRealCallbacks = Collections.emptyList();
    }

    /**
     * Creates a new BatchedWriteCallback.
     * 
     * @param databaseName
     *            The name of the database.
     * @param realCallbacks
     *            The list of callbacks. One for each write.
     * @param write
     *            The original write.
     * @param bundles
     *            The bundled writes.
     */
    public BatchedWriteCallback(final String databaseName,
            final List<Callback<Reply>> realCallbacks,
            final BatchedWrite write, final List<Bundle> bundles) {
        super(null);

        myDatabaseName = databaseName;
        myWrite = write;
        myClient = null;
        myBundles = Collections
                .unmodifiableList(new ArrayList<BatchedWrite.Bundle>(bundles));
        myCollection = null;
        myOperations = Collections.emptyList();

        myPendingBundles = new LinkedList<BatchedWrite.Bundle>(myBundles);
        myPendingOperations = Collections.emptyList();

        myFinished = 0;
        myFinished = 0;

        myFailedOperations = new IdentityHashMap<WriteOperation, Throwable>();
        mySkippedOperations = null;

        myRealCallbacks = new ArrayList<Callback<Reply>>(realCallbacks);

        int count = 0;
        for (final Bundle b : myBundles) {
            count += b.getWrites().size();
        }
        Assertions.assertThat(
                myRealCallbacks.size() == count,
                "There nust be an operation (" + count
                        + ") in a bundle for each callback ("
                        + myRealCallbacks.size() + ").");
    }

    /**
     * Sends the next set of operations to the server.
     */
    public void send() {

        List<BatchedWrite.Bundle> toSendBundles = Collections.emptyList();
        List<WriteOperation> toSendOperations = Collections.emptyList();
        Durability durability = null;

        synchronized (this) {
            if (!myPendingBundles.isEmpty()) {
                List<BatchedWrite.Bundle> toSend = myPendingBundles;
                if (BatchedWriteMode.SERIALIZE_AND_STOP.equals(myWrite
                        .getMode())) {
                    toSend = myPendingBundles.subList(0, 1);
                }

                // Clear toSend before sending so the callbacks see the right
                // state for the bundles.
                toSendBundles = new ArrayList<BatchedWrite.Bundle>(toSend);
                toSend.clear();
            }
            else if (!myPendingOperations.isEmpty()) {
                List<WriteOperation> toSend = myPendingOperations;
                if (BatchedWriteMode.SERIALIZE_AND_STOP.equals(myWrite
                        .getMode())) {
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
            }
        } // Release lock.

        // Release the lock before sending to avoid deadlock in processing
        // replies.

        // Batches....
        for (final BatchedWrite.Bundle bundle : toSendBundles) {
            final Command commandMsg = new Command(myDatabaseName,
                    bundle.getCommand(), ReadPreference.PRIMARY,
                    VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

            // Our documents may be bigger than normally allowed...
            commandMsg.setAllowJumbo(true);

            myClient.send(commandMsg, new BundleCallback(bundle));
        }

        // Operations...
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
     * Sets the client to use to send the bundled writes.
     * 
     * @param client
     *            The new client for the batch.
     */
    public void setClient(final Client client) {
        myClient = client;
    }

    /**
     * Callback for a bundle of write operations sent via the write commands.
     * 
     * @param bundle
     *            The bundle of write operations.
     * @param result
     *            The result of the write operations.
     */
    protected synchronized void callback(final Bundle bundle, final Reply result) {
        final MongoDbException error = asError(result);
        if (error != null) {
            // Everything failed...
            exception(bundle, error);
        }
        else {
            myFinished += 1;
            try {
                final Long n = convert(result);
                publish(bundle, n.longValue());

                myN += n.longValue();

                if (failedDurability(bundle, result)
                        || failedWrites(bundle, result)
                        || (myFinished == myBundles.size())) {
                    publishResults();
                }
                else if (!myPendingBundles.isEmpty()) {
                    send();
                }
            }
            catch (final MongoDbException e2) {
                // Everything failed...
                exception(bundle, e2);
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

        publish(operation, result);

        if (!myPendingOperations.isEmpty()) {
            send();
        }
        else if (myFinished == myOperations.size()) {
            publishResults();
        }
    }

    /**
     * Callback for a bundle of write operations sent via the write commands has
     * failed.
     * 
     * @param bundle
     *            The bundle of write operations.
     * @param thrown
     *            The error for the operations.
     */
    protected synchronized void exception(final Bundle bundle,
            final Throwable thrown) {
        myFinished += 1;
        for (final WriteOperation operation : bundle.getWrites()) {
            myFailedOperations.put(operation, thrown);
        }

        if (myWrite.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP) {
            publishResults();
        }
        else if (!myPendingBundles.isEmpty()) {
            send();
        }
        else if (myFinished == myBundles.size()) {
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
        else if (!myPendingOperations.isEmpty()) {
            send();
        }
        else if (myFinished == myOperations.size()) {
            publishResults();
        }

    }

    /**
     * Checks for a failure in the durability requirements (e.g., did not
     * replicate to sufficient servers within the timeout) and updates the
     * failed operations map if any are found.
     * 
     * @param bundle
     *            The bundle for the reply.
     * @param reply
     *            The reply from the server.
     * @return True if there are failed writes and we should not send any
     *         additional requests.
     */
    private boolean failedDurability(final Bundle bundle, final Reply reply) {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final DocumentElement error = doc.get(DocumentElement.class,
                    "writeConcernError");
            if (error != null) {
                final int code = toInt(error.get(NumericElement.class, "code"));
                final String errmsg = asString(error.get(Element.class,
                        "errmsg"));
                final MongoDbException exception = asError(reply, 0, code,
                        true, errmsg, null);
                for (final WriteOperation op : bundle.getWrites()) {
                    myFailedOperations.put(op, exception);
                }
            }
        }

        return (myWrite.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP)
                && !myFailedOperations.isEmpty();
    }

    /**
     * Checks for individual {@code writeErrors} and updates the failed
     * operations map if any are found.
     * 
     * @param bundle
     *            The bundle for the reply.
     * @param reply
     *            The reply from the server.
     * @return True if there are failed writes and we should not send any
     *         additional requests.
     */
    private boolean failedWrites(final Bundle bundle, final Reply reply) {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final ArrayElement errors = doc.get(ArrayElement.class,
                    "writeErrors");
            if (errors != null) {
                final List<WriteOperation> operations = bundle.getWrites();
                for (final DocumentElement error : errors.find(
                        DocumentElement.class, ".*")) {
                    final int index = toInt(error.get(NumericElement.class,
                            "index"));
                    final int code = toInt(error.get(NumericElement.class,
                            "code"));
                    final String errmsg = asString(error.get(Element.class,
                            "errmsg"));

                    if ((0 <= index) && (index < operations.size())) {
                        final WriteOperation op = operations.get(index);

                        myFailedOperations.put(op,
                                asError(reply, 0, code, false, errmsg, null));

                        if (myWrite.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP) {
                            mySkippedOperations = new ArrayList<WriteOperation>();
                            mySkippedOperations.addAll(operations.subList(
                                    index + 1, operations.size()));
                        }
                    }
                }
            }
        }

        return (myWrite.getMode() == BatchedWriteMode.SERIALIZE_AND_STOP)
                && !myFailedOperations.isEmpty();
    }

    /**
     * Publishes the results for an individual bundle.
     * 
     * @param bundle
     *            The bundle that we received the results for.
     * @param value
     *            The value from the result.
     */
    private void publish(final Bundle bundle, final long value) {
        if (myForwardCallback == null) {
            // Publish to each callback.
            final Document doc = BuilderFactory.start().add("ok", 1)
                    .add("n", value).build();
            final Reply reply = new Reply(0, 0, 0,
                    Collections.singletonList(doc), false, false, false, false);
            int index = 0;
            for (final Bundle b : myBundles) {
                final int count = b.getWrites().size();
                for (int i = 0; i < count; ++i) {
                    // Bundles can compare logically the same but still be
                    // different.
                    if (b == bundle) {
                        // Replace the callback to avoid double calls.
                        final Callback<Reply> cb = myRealCallbacks.set(index,
                                NO_OP);
                        if (cb != null) {
                            // Worked
                            cb.callback(reply);
                        }
                    }

                    // Next...
                    index += 1;
                }

                if (b == bundle) {
                    return;
                }
            }
        }
    }

    /**
     * Publishes the results for an individual write operation.
     * 
     * @param operation
     *            The operation that we received the results for.
     * @param value
     *            The value from the result.
     */
    private void publish(final WriteOperation operation, final long value) {
        if (myForwardCallback == null) {
            // Publish to each callback.
            final Document doc = BuilderFactory.start().add("ok", 1)
                    .add("n", value).build();
            final Reply reply = new Reply(0, 0, 0,
                    Collections.singletonList(doc), false, false, false, false);
            int index = 0;
            for (final WriteOperation op : myOperations) {
                // WriteOperation can compare logically the same but still be
                // different.
                if (op == operation) {
                    // Replace the callback to avoid double calls.
                    final Callback<Reply> cb = myRealCallbacks
                            .set(index, NO_OP);
                    if (cb != null) {
                        // Worked
                        cb.callback(reply);
                    }
                    break;
                }
                // Next...
                index += 1;
            }
        }
    }

    /**
     * Publishes the final results.
     */
    private void publishResults() {
        if (myFailedOperations.isEmpty()) {
            if (myForwardCallback != null) {
                myForwardCallback.callback(Long.valueOf(myN));
            }
            else {
                // Publish to each callback.
                final Document doc = BuilderFactory.start().add("ok", 1)
                        .add("n", myN).build();
                final Reply reply = new Reply(0, 0, 0,
                        Collections.singletonList(doc), false, false, false,
                        false);
                for (final Callback<Reply> cb : myRealCallbacks) {
                    if (cb != null) {
                        cb.callback(reply);
                    }
                }
            }
        }
        else {
            if (mySkippedOperations == null) {
                mySkippedOperations = new ArrayList<WriteOperation>();
            }
            mySkippedOperations.addAll(myPendingOperations);
            for (final Bundle pending : myPendingBundles) {
                mySkippedOperations.addAll(pending.getWrites());
            }

            if (myForwardCallback != null) {
                myForwardCallback.exception(new BatchedWriteException(myWrite,
                        myN, mySkippedOperations, myFailedOperations));
            }
            else {
                // Publish to each callback.
                final List<WriteOperation> emptySkipped = Collections
                        .emptyList();
                final Map<WriteOperation, Throwable> emptyError = Collections
                        .emptyMap();

                // For fast lookup and lookup by identity.
                final Set<WriteOperation> skipped = Collections
                        .newSetFromMap(new IdentityHashMap<WriteOperation, Boolean>());
                skipped.addAll(mySkippedOperations);

                final Document doc = BuilderFactory.start().add("ok", 1)
                        .add("n", myN).build();
                final Reply reply = new Reply(0, 0, 0,
                        Collections.singletonList(doc), false, false, false,
                        false);

                int index = 0;
                for (final Bundle b : myBundles) {
                    for (final WriteOperation op : b.getWrites()) {
                        final Callback<Reply> cb = myRealCallbacks.get(index);

                        if (cb != null) {
                            // Did this write fail?
                            final Throwable thrown = myFailedOperations.get(op);
                            if (thrown != null) {
                                cb.exception(new BatchedWriteException(myWrite,
                                        myN, emptySkipped, Collections
                                                .singletonMap(op, thrown)));
                            }
                            else if (skipped.contains(op)) {
                                // Skipped the write.
                                cb.exception(new BatchedWriteException(myWrite,
                                        myN, Collections.singletonList(op),
                                        emptyError));
                            }
                            else {
                                // Worked
                                cb.callback(reply);
                            }
                        }

                        // Next...
                        index += 1;
                    }
                }
            }
        }
    }

    /**
     * BundleCallback provides the callback for a single batched write.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    /* package */class BundleCallback implements ReplyCallback {

        /**
         * The bundle of operations this callback is waiting for the reply from.
         */
        private final Bundle myBundle;

        /**
         * Creates a new BatchedWriteBundleCallback.
         * 
         * @param bundle
         *            The bundle of operations this callback is waiting for the
         *            reply from.
         */
        public BundleCallback(final Bundle bundle) {
            myBundle = bundle;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward the results to the parent callback.
         * </p>
         */
        @Override
        public void callback(final Reply result) {
            BatchedWriteCallback.this.callback(myBundle, result);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward the error to the parent callback.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            BatchedWriteCallback.this.exception(myBundle, thrown);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to return false.
         * </p>
         */
        @Override
        public boolean isLightWeight() {
            return false;
        }
    }

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
            BatchedWriteCallback.this.callback(myOperation, result.longValue());
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to forward the error to the parent callback.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            BatchedWriteCallback.this.exception(myOperation, thrown);
        }
    }

    /**
     * CallbackImplementation provides a no-op callback.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    /* package */final static class NoOpCallback implements Callback<Reply> {
        @Override
        public void callback(final Reply result) {
            // Nothing.
        }

        @Override
        public void exception(final Throwable thrown) {
            // Nothing.
        }
    }
}
