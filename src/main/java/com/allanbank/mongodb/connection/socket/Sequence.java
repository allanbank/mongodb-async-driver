/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import java.util.concurrent.atomic.AtomicLongArray;

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

    /** The atomic array of long values. */
    private final AtomicLongArray myPaddedValue = new AtomicLongArray(30);

    /**
     * Create a sequence with a specified initial value.
     * 
     * @param initialValue
     *            The initial value for this sequence.
     */
    public Sequence(final long initialValue) {
        myPaddedValue.set(RESERVE_OFFSET, initialValue);
        myPaddedValue.set(RELEASE_OFFSET, initialValue);
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
            // Let another thread make progress.
            Thread.yield();

            releaseValue = myPaddedValue.get(RELEASE_OFFSET);
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
}