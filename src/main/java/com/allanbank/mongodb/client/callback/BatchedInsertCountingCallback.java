/*
 * #%L
 * BatchedInsertCountingCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
public class BatchedInsertCountingCallback
        implements Callback<Reply> {

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
        boolean publish;
        synchronized (this) {
            myCount += 1;
            publish = (myCount == myExpectedCount);
        }

        if (publish) {
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
        boolean publish;
        synchronized (this) {
            myFailureCount += 1;
            myCount += 1;
            publish = (myCount == myExpectedCount);

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

        if (publish) {
            publish();
        }
    }

    /**
     * Publishes the final results to {@link #myForwardCallback}.
     */
    private void publish() {
        Reply reply = null;
        BatchedWriteException error = null;
        synchronized (this) {
            if (myFailureCount == 0) {
                final Document doc = BuilderFactory.start().add("ok", 1)
                        .add("n", myExpectedCount).build();
                reply = new Reply(0, 0, 0, Collections.singletonList(doc),
                        false, false, false, false);
            }
            else {
                error = new BatchedWriteException(myLastWrite,
                        (myExpectedCount - myFailureCount), mySkipped, myErrors);
            }
        }

        // Reply outside the lock.
        if (reply != null) {
            myForwardCallback.callback(reply);
        }
        else {
            myForwardCallback.exception(error);
        }
    }

}
