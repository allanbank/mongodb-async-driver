/*
 * #%L
 * FutureCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ListenableFuture;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.client.callback.ReplyHandler;
import com.allanbank.mongodb.util.Assertions;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Implementation of a {@link Callback} and {@link ListenableFuture} interfaces.
 * Used to convert a {@link Callback} into a {@link ListenableFuture} for
 * returning to callers.
 *
 * @param <V>
 *            The type for the set value.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FutureCallback<V>
        implements ListenableFuture<V>, Callback<V> {

    /** Logger to log exceptions caught when running myPendingListeners. */
    public static final Log LOG = LogFactory.getLog(FutureCallback.class);

    /** Amount of time to spin before yielding. Set to 1/100 of a millisecond. */
    public static final long SPIN_TIME_NS = TimeUnit.MILLISECONDS.toNanos(1) / 100;

    /** Number of times to spin before trying something different. */
    private static final int SPIN_ITERATIONS = 10000;

    /** Amount of time to spin/yield before waiting. Set to 1/2 millisecond. */
    private static final long YIELD_TIME_NS = TimeUnit.MILLISECONDS.toNanos(1) >> 1;

    /** The type of lock to use when waiting for the future to be fulfilled. */
    private final LockType myLockType;

    /** The runnable, executor pairs to execute within a singly linked list. */
    private AtomicReference<PendingListener> myPendingListeners;

    /** Synchronization control for this Future. */
    private final Sync<V> mySync;

    /**
     * Create a new FutureCallback.
     */
    public FutureCallback() {
        this(LockType.MUTEX);
    }

    /**
     * Create a new FutureCallback.
     *
     * @param lockType
     *            The type of lock to use when waiting for the future to be
     *            fulfilled.
     */
    public FutureCallback(final LockType lockType) {
        mySync = new Sync<V>();
        myLockType = lockType;
        myPendingListeners = new AtomicReference<PendingListener>(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(final Runnable runnable, final Executor executor) {
        Assertions.assertNotNull(runnable, "Runnable is null.");
        Assertions.assertNotNull(executor, "Executor is null.");

        if (!isDone()) {
            PendingListener existing = myPendingListeners.get();
            PendingListener listener = new PendingListener(runnable, executor,
                    existing);

            while (!myPendingListeners.compareAndSet(existing, listener)) {
                existing = myPendingListeners.get();
                listener = new PendingListener(runnable, executor, existing);
            }

            if (isDone()) {
                execute();
            }
        }
        else {
            // Run the executor.
            execute(executor, runnable);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets the value for the future and triggers any pending {@link #get} to
     * return.
     * </p>
     *
     * @see Callback#callback
     */
    @Override
    public void callback(final V result) {
        final boolean set = mySync.set(result);
        if (set) {
            execute();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * If not cancelled and the callback has not completed then cancels the
     * future, triggers the return of any pending {@link #get()} and returns
     * true. Otherwise returns false. This does not stop the related MongoDB
     * invocation.
     * </p>
     *
     * @see Future#cancel(boolean)
     */
    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        if (!mySync.cancel(mayInterruptIfRunning)) {
            return false;
        }
        execute();

        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets the exception for the future and triggers any pending {@link #get}
     * to throw a {@link ExecutionException}.
     * </p>
     *
     * @see Callback#exception
     */
    @Override
    public void exception(final Throwable thrown) {
        Assertions.assertNotNull(thrown, "Cannot set a null exception.");

        final boolean set = mySync.setException(thrown);
        if (set) {
            execute();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the value set via the {@link Callback}.
     * </p>
     *
     * @see Future#get()
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {

        if (myLockType == LockType.LOW_LATENCY_SPIN) {
            long now = 0;
            long spinDeadline = 1;
            long yeildDeadline = 1;
            while ((now < yeildDeadline) && !isDone()) {
                for (int i = 0; (i < SPIN_ITERATIONS) && !isDone(); ++i) {
                    // Hard spin...
                }

                if (!isDone()) {
                    // Pause?
                    now = System.nanoTime();
                    if (spinDeadline == 1) {
                        spinDeadline = now + SPIN_TIME_NS;
                        yeildDeadline = now + YIELD_TIME_NS;
                        // First time yield to allow other threads to do their
                        // work...
                        Thread.yield();
                    }
                    else {
                        if ((spinDeadline < now) && (now < yeildDeadline)) {
                            Thread.yield();
                        }
                    }
                }
            }
        }

        final long shortPause = TimeUnit.MILLISECONDS.toNanos(10);
        while (true) {
            try {
                // Either the value is available and the get() will not block
                // or we have spun for long enough and it is time to block.
                return mySync.get(shortPause);
            }
            catch (final TimeoutException te) {
                ReplyHandler.tryReceive();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException, ExecutionException {
        long now = System.nanoTime();
        final long deadline = now + unit.toNanos(timeout);
        final long shortPause = TimeUnit.MILLISECONDS.toNanos(10);
        while (true) {
            try {
                // Wait for the result.
                return mySync.get(Math.min((deadline - now), shortPause));
            }
            catch (final TimeoutException te) {
                // Check if we should receive.
                now = System.nanoTime();
                if (now < deadline) {
                    ReplyHandler.tryReceive();
                }
                else {
                    throw te;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if {@link #cancel(boolean)} has been called.
     * </p>
     *
     * @see Future#isCancelled()
     */
    @Override
    public boolean isCancelled() {
        return mySync.isCancelled();
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if a value has been set via the the {@link Callback} interface or
     * the {@link Future} has been {@link #cancel(boolean) cancelled}.
     * </p>
     *
     * @see Future#isDone()
     */
    @Override
    public boolean isDone() {
        return mySync.isDone();
    }

    /**
     * Runs this execution list, executing all existing pairs.
     * <p>
     * All callers of this method will drain the list of listeners.
     * </p>
     */
    protected void execute() {
        PendingListener toRun;
        PendingListener next;

        // Keep running until the list is exhausted.
        do {

            // Pick the next item from the list.
            do {
                toRun = myPendingListeners.get();
                next = (toRun != null) ? toRun.myNext : null;
            }
            while (!myPendingListeners.compareAndSet(toRun, next));

            // Run this item - if it exists.
            if (toRun != null) {
                execute(toRun.myExecutor, toRun.myRunnable);
            }
        }
        while (toRun != null);
    }

    /**
     * Execute the {@link Runnable} with the {@link Executor} suppressing
     * exceptions.
     *
     * @param executor
     *            The executor to use.
     * @param runnable
     *            The {@link Runnable} to execute.
     */
    private void execute(final Executor executor, final Runnable runnable) {
        try {
            executor.execute(runnable);
        }
        catch (final RuntimeException e) {
            LOG.error(e, "Exception running a FutureListener's runnable {} "
                    + "with executor {}", runnable, executor);
        }
    }

    /**
     * PendingListener an immutable element in the list of listeners.
     *
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    /* package */static final class PendingListener {

        /** The executor to use to run the {@link Runnable}. */
        /* package */final Executor myExecutor;

        /** The next item to execute. */
        /* package */final PendingListener myNext;

        /** The listener's {@link Runnable}. */
        /* package */final Runnable myRunnable;

        /**
         * Creates a new PendingListener.
         *
         * @param runnable
         *            The listener's {@link Runnable}.
         * @param executor
         *            The executor to use to run the {@link Runnable}.
         * @param next
         *            The next item to execute.
         */
        /* package */PendingListener(final Runnable runnable,
                final Executor executor, final PendingListener next) {
            myRunnable = runnable;
            myExecutor = executor;
            myNext = next;
        }
    }

    /**
     * Tracks the state of the Future via the {@link AbstractQueuedSynchronizer}
     * model. The state starts in the {@link #RUNNING} state. The first thread
     * to complete the future moves the state to the {@link #COMPLETING} state,
     * sets the value and then sets the appropriate final state.
     *
     * @param <V>
     *            The type of value for the future.
     */
    /* package */static final class Sync<V>
            extends AbstractQueuedSynchronizer {

        /** State to represent the future was canceled. */
        /* package */static final int CANCELED = 4;

        /** State to represent the value has been set. */
        /* package */static final int COMPLETED = 2;

        /** State set while the value is being set. */
        /* package */static final int COMPLETING = 1;

        /** State to represent the future was interrupted. */
        /* package */static final int INTERRUPTED = 8;

        /** The initial running state. */
        /* package */static final int RUNNING = 0;

        /** The unused value passed to {@link #acquire(int)} methods. */
        /* package */static final int UNUSED = -1;

        /** Serialization version of the class. */
        private static final long serialVersionUID = -9189950787072982459L;

        /** The exception for the future. */
        private Throwable myException;

        /** The value set in the future. */
        private V myValue;

        /**
         * Creates a new Sync.
         */
        /* package */Sync() {
            myValue = null;
            myException = null;
        }

        /**
         * Acquisition succeeds if we are done, otherwise fail.
         */
        @Override
        protected int tryAcquireShared(final int ignored) {
            if (isDone()) {
                return 1;
            }
            return -1;
        }

        /**
         * We always allow a release to finish. We define it to represent that a
         * state transition completed.
         */
        @Override
        protected boolean tryReleaseShared(final int finalState) {
            setState(finalState);
            return true;
        }

        /**
         * Move to the CANCELED or INTERRUPTED state.
         *
         * @param interrupt
         *            If we are interrupted.
         * @return If the cancel worked / won.
         */
        /* package */boolean cancel(final boolean interrupt) {
            return complete(null, null, interrupt ? INTERRUPTED : CANCELED);
        }

        /**
         * Blocks until the future {@link #complete(Object, Throwable, int)
         * completes}.
         *
         * @return The value set for the future.
         * @throws CancellationException
         *             If the future was canceled.
         * @throws ExecutionException
         *             If the future failed due to an exception.
         * @throws InterruptedException
         *             If these call is interrupted.
         */
        /* package */V get() throws CancellationException, ExecutionException,
                InterruptedException {

            // Acquire the shared lock allowing interruption.
            acquireSharedInterruptibly(UNUSED);

            return getValue();
        }

        /**
         * Blocks until the future {@link #complete(Object, Throwable, int)
         * completes} or the number of nano-seconds expires.
         *
         * @param nanos
         *            The number of nano-seconds to wait.
         * @return The value set for the future.
         * @throws TimeoutException
         *             If this call time expires.
         * @throws CancellationException
         *             If the future was canceled.
         * @throws ExecutionException
         *             If the future failed due to an exception.
         * @throws InterruptedException
         *             If these call is interrupted.
         */
        /* package */V get(final long nanos) throws TimeoutException,
                CancellationException, ExecutionException, InterruptedException {

            // Attempt to acquire the shared lock with a timeout.
            if (!tryAcquireSharedNanos(UNUSED, nanos)) {
                throw new TimeoutException("Timeout waiting for task.");
            }

            return getValue();
        }

        /**
         * Checks if the state is {@link #CANCELED} or {@link #INTERRUPTED}.
         *
         * @return True if the future state is {@link #CANCELED} or
         *         {@link #INTERRUPTED}.
         */
        /* package */boolean isCancelled() {
            return (getState() & (CANCELED | INTERRUPTED)) != 0;
        }

        /**
         * Checks if the state is {@link #COMPLETED}, {@link #CANCELED} or
         * {@link #INTERRUPTED}.
         *
         * @return True if the future state is {@link #COMPLETED},
         *         {@link #CANCELED} or {@link #INTERRUPTED}.
         */
        /* package */boolean isDone() {
            return (getState() & (COMPLETED | CANCELED | INTERRUPTED)) != 0;
        }

        /**
         * Move to the COMPLETED state and set the value.
         *
         * @param value
         *            The value to set.
         * @return If the set worked / won.
         */
        /* package */boolean set(final V value) {
            return complete(value, null, COMPLETED);
        }

        /**
         * Move to the COMPLETED state and set the exception value.
         *
         * @param thrown
         *            The exception to set.
         * @return If the set worked / won.
         */
        /* package */boolean setException(final Throwable thrown) {
            return complete(null, thrown, COMPLETED);
        }

        /**
         * Completes the future.
         *
         * @param value
         *            The value to set as the result of the future.
         * @param thrown
         *            the exception to set as the result of the future.
         * @param finalState
         *            the state to transition to.
         * @return Returns true if the completion was successful / won.
         */
        private boolean complete(final V value, final Throwable thrown,
                final int finalState) {

            // Move to COMPLETING to see if we are the first to complete.
            final boolean won = compareAndSetState(RUNNING, COMPLETING);
            if (won) {
                this.myValue = value;
                this.myException = ((finalState & (CANCELED | INTERRUPTED)) != 0) ? new CancellationException(
                        "Future was canceled.") : thrown;

                // Release all of the waiting threads.
                releaseShared(finalState);
            }
            else if (getState() == COMPLETING) {
                // Block until done.
                acquireShared(UNUSED);
            }

            return won;
        }

        /**
         * Implementation to get the future's value.
         *
         * @return The value set for the future.
         * @throws CancellationException
         *             If the future was canceled.
         * @throws ExecutionException
         *             If the future failed due to an exception.
         */
        private V getValue() throws CancellationException, ExecutionException {
            final int state = getState();
            switch (state) {
            case COMPLETED:
                if (myException != null) {
                    throw new ExecutionException(myException);
                }
                return myValue;

            case CANCELED:
            case INTERRUPTED:
                final CancellationException cancellation = new CancellationException(
                        "Future was canceled.");
                cancellation.initCause(myException);

                throw cancellation;

            default:
                throw new IllegalStateException("Sync in invalid state: "
                        + state);
            }
        }
    }

}
