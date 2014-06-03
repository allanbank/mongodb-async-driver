/*
 * #%L
 * Sequence.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.socket;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.client.message.PendingMessageQueue;

/**
 * Sequence provides the ability to synchronize the access to the socket's
 * output stream. The thread starts by reserving a position via the reserve
 * method and then prepares to send the messages. It will then wait for its turn
 * to send and finally release the sequence.
 * <p>
 * We use an array of longs to avoid false sharing.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class Sequence {

    /** The offset of the release value. */
    private static final int RELEASE_OFFSET = 15 + 7;

    /** The offset of the reserve value. */
    private static final int RESERVE_OFFSET = 7;

    /** Amount of time to spin/yield before waiting. Set to 1/2 millisecond. */
    private static final long YIELD_TIME_NS = PendingMessageQueue.YIELD_TIME_NS;

    /** The condition used when there are waiters. */
    private final Condition myCondition;

    /** The mutex used with the sequence. */
    private final Lock myLock;

    /** The lock type to use with the sequence. */
    private final LockType myLockType;

    /** The atomic array of long values. */
    private final AtomicLongArray myPaddedValue = new AtomicLongArray(30);

    /**
     * The map of waiters and the condition they are waiting on to avoid the
     * thundering herd. Only access while holding the {@link #myLock lock}.
     */
    private final SortedMap<Long, Condition> myWaiters;

    /** Tracks how many threads are waiting for a message or a space to open. */
    private final AtomicInteger myWaiting;

    /**
     * Create a sequence with a specified initial value.
     * 
     * @param initialValue
     *            The initial value for this sequence.
     */
    public Sequence(final long initialValue) {
        this(initialValue, LockType.MUTEX);
    }

    /**
     * Create a sequence with a specified initial value.
     * 
     * @param initialValue
     *            The initial value for this sequence.
     * @param lockType
     *            The lock type to use with the sequence.
     */
    public Sequence(final long initialValue, final LockType lockType) {
        myPaddedValue.set(RESERVE_OFFSET, initialValue);
        myPaddedValue.set(RELEASE_OFFSET, initialValue);

        myLockType = lockType;

        myLock = new ReentrantLock(true);
        myCondition = myLock.newCondition();
        myWaiting = new AtomicInteger(0);
        myWaiters = new TreeMap<Long, Condition>();
    }

    /**
     * Returns true if the sequence is idle (reserve == release).
     * 
     * @return True if the sequence is idle.
     */
    public boolean isIdle() {
        final long reserve = myPaddedValue.get(RESERVE_OFFSET);
        final long release = myPaddedValue.get(RELEASE_OFFSET);
        return (reserve == release);
    }

    /**
     * Checks if there is a waiter for the sequence to be released.
     * 
     * @param expectedReserve
     *            The expected value for the reserve if there is no waiter.
     * @return True if there is a waiter (e.g., the reserve has advanced).
     */
    public boolean noWaiter(final long expectedReserve) {
        final long reserve = myPaddedValue.get(RESERVE_OFFSET);

        return (reserve == expectedReserve);
    }

    /**
     * Release the position in the sequence.
     * 
     * @param expectedValue
     *            The expected/reserved value for the sequence.
     * @param newValue
     *            The new value for the sequence.
     */
    public void release(final long expectedValue, final long newValue) {
        while (!compareAndSetRelease(expectedValue, newValue)) {
            // Let another thread make progress - should not really spin if we
            // did a waitFor.
            Thread.yield();
        }
        notifyWaiters();
    }

    /**
     * Reserves a spot in the sequence for the messages to be sent.
     * 
     * @param numberOfMessages
     *            The number of messages to be sent.
     * @return The current value of the sequence.
     */
    public long reserve(final long numberOfMessages) {
        long current;
        long next;

        do {
            current = myPaddedValue.get(RESERVE_OFFSET);
            next = current + numberOfMessages;
        }
        while (!compareAndSetReserve(current, next));

        return current;
    }

    /**
     * Waits for the reserved sequence to be released.
     * 
     * @param wanted
     *            The sequence to wait to be released.
     */
    public void waitFor(final long wanted) {
        long releaseValue = myPaddedValue.get(RELEASE_OFFSET);
        while (releaseValue != wanted) {
            if (myLockType == LockType.LOW_LATENCY_SPIN) {
                long now = System.nanoTime();
                final long yeildDeadline = now + YIELD_TIME_NS;

                releaseValue = myPaddedValue.get(RELEASE_OFFSET);
                while ((now < yeildDeadline) && (releaseValue != wanted)) {
                    // Let another thread make progress.
                    Thread.yield();
                    now = System.nanoTime();
                    releaseValue = myPaddedValue.get(RELEASE_OFFSET);
                }
            }

            // Block.
            if (releaseValue != wanted) {
                final Long key = Long.valueOf(wanted);
                Condition localCondition = myCondition;
                try {
                    final int waitCount = myWaiting.incrementAndGet();
                    myLock.lock();

                    // Second tier try for FindBugs to see the unlock.
                    try {
                        // Check for more than 1 waiter. If so stand in line via
                        // the waiters map. (This will wake threads in the order
                        // they should be processed.)
                        if (waitCount > 1) {
                            localCondition = myLock.newCondition();
                            myWaiters.put(key, localCondition);
                        }

                        releaseValue = myPaddedValue.get(RELEASE_OFFSET);
                        while (releaseValue != wanted) {
                            localCondition.awaitUninterruptibly();
                            releaseValue = myPaddedValue.get(RELEASE_OFFSET);
                        }
                    }
                    finally {
                        if (localCondition != myCondition) {
                            myWaiters.remove(key);
                        }
                    }
                }
                finally {
                    myLock.unlock();
                    myWaiting.decrementAndGet();
                }
            }
        }
    }

    /**
     * Perform a compare and set operation on the sequence release position.
     * 
     * @param expectedValue
     *            The expected current value.
     * @param newValue
     *            The value to update to.
     * @return true if the operation succeeds, false otherwise.
     */
    private boolean compareAndSetRelease(final long expectedValue,
            final long newValue) {
        return myPaddedValue.compareAndSet(RELEASE_OFFSET, expectedValue,
                newValue);
    }

    /**
     * Perform a compare and set operation on the sequence reserve position.
     * 
     * @param expectedValue
     *            The expected current value.
     * @param newValue
     *            The value to update to.
     * @return true if the operation succeeds, false otherwise.
     */
    private boolean compareAndSetReserve(final long expectedValue,
            final long newValue) {
        return myPaddedValue.compareAndSet(RESERVE_OFFSET, expectedValue,
                newValue);
    }

    /**
     * Notifies the waiting threads that the state of the sequence has changed.
     */
    private void notifyWaiters() {
        if (myWaiting.get() > 0) {
            try {
                myLock.lock();

                // Wake the reused condition.
                myCondition.signalAll();

                // Wake up the condition with the lowest wanted.
                // No Thundering Herd!
                if (!myWaiters.isEmpty()) {
                    myWaiters.get(myWaiters.firstKey()).signalAll();
                }
            }
            finally {
                myLock.unlock();
            }
        }
    }
}