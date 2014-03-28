/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.BatchedWriteException;

/**
 * BatchedInsertCountingCallback is designed to work with the
 * {@link BatchedWriteCallback}. This callback can be used as the callback for a
 * series of individual writes and will coalesce the results into a single
 * result based on a an expected number of callbacks.
 * <p>
 * The class does not track the input {@code n} value and instead always returns
 * an N value based on the expected count. That limits the utility of this class
 * to inserts.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedInsertCountingCallback implements Callback<Reply> {

    /** The count of the number of callbacks received so far. */
    private int myCount = 0;

    /** The failed operations. */
    private Map<WriteOperation, Throwable> myErrors;

    /** The expected number of callbacks. */
    private final int myExpectedCount;

    /**
     * The count of the number of failure ({@link #exception(Throwable)})
     * callbacks received so far.
     */
    private int myFailureCount = 0;

    /** The callback to notify with the final results once we receive them all. */
    private final Callback<Reply> myForwardCallback;

    /** The last batched write that failed. */
    private BatchedWrite myLastWrite;

    /** The skipped operations. */
    private List<WriteOperation> mySkipped;

    /**
     * Creates a new CountingCallback.
     * 
     * @param forwardCallback
     *            The callback to notify with the final results once we receive
     *            them all.
     * @param expectedCount
     *            The expected number of callbacks.
     */
    public BatchedInsertCountingCallback(final Callback<Reply> forwardCallback,
            final int expectedCount) {
        myForwardCallback = forwardCallback;
        myExpectedCount = expectedCount;
        myCount = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to increment the count and when the max is reached forward the
     * final results.
     * </p>
     */
    @Override
    public void callback(final Reply result) {
        int count;
        synchronized (this) {
            myCount += 1;
            count = myCount;
        }

        if (count == myExpectedCount) {
            publish();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to record the exception details.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        int count;
        synchronized (this) {
            myFailureCount += 1;
            myCount += 1;
            count = myCount;

            if (mySkipped == null) {
                mySkipped = new ArrayList<WriteOperation>();
                myErrors = new IdentityHashMap<WriteOperation, Throwable>();
            }

            if (thrown instanceof BatchedWriteException) {
                final BatchedWriteException errors = (BatchedWriteException) thrown;

                myLastWrite = errors.getWrite();
                mySkipped.addAll(errors.getSkipped());
                myErrors.putAll(errors.getErrors());
            }
        }

        if (count == myExpectedCount) {
            publish();
        }
    }

    /**
     * Publishes the final results to {@link #myForwardCallback}.
     */
    private void publish() {
        if (myFailureCount == 0) {
            final Document doc = BuilderFactory.start().add("ok", 1)
                    .add("n", myExpectedCount).build();
            final Reply reply = new Reply(0, 0, 0,
                    Collections.singletonList(doc), false, false, false, false);

            myForwardCallback.callback(reply);
        }
        else {
            myForwardCallback.exception(new BatchedWriteException(myLastWrite,
                    (myExpectedCount - myFailureCount), mySkipped, myErrors));
        }
    }

}
