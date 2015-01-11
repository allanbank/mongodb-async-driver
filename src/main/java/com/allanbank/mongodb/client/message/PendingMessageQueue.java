/*
 * #%L
 * PendingMessageQueue.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.message;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;

/**
 * PendingMessageQueue provides an optimized queue for pending messages inspired
 * by the Disruptor project.
 * <p>
 * To reduce thread contention the queue uses a set of integer values to track
 * the position of the ready messages (the last message that is ready to be
 * read), reserve (the first message that can be reserved to be written to), and
 * the take (the next (first) message to be read). For an infinite queue the
 * following invariant holds: <blockquote>
 *
 * <pre>
 * <code>
 * take &lt; readyBefore &lt;= reserve
 * </code>
 * </pre>
 *
 * </blockquote> To make handling a limited size queue easier the size of the
 * queue is forced to power of 2 less than {@value #MAX_SIZE}. The roll over can
 * then be handled with a simple mask operation.
 * </p>
 * <p>
 * Rather than allocate a pending message per request we use an array of
 * pre-allocated PendingMessages and copy the data into and out of the objects.
 * this has a net positive effect on object allocation and garbage collection
 * time at the cost of a longer initialization.
 * </p>
 * <p>
 * Lastly, This queue assumes there is a single consumer of messages. This is
 * true for the driver's use case but don't copy the code and expect it to work
 * with multiple consumers. The consumer should use the following basic
 * structure: <blockquote>
 *
 * <pre>
 * <code>
 * PendingMessage pm = new {@link PendingMessage}();
 * 
 * queue.take(pm); // Blocks.
 * // Handle the message.
 * 
 * // or
 * 
 * if( queue.poll(pm) ) { // Non-blocking.
 *    // Handle The Message.
 * }
 * </code>
 * </pre>
 *
 * </blockquote>
 * </p>
 * <p>
 * <b>Warning: </b> This class has been carefully tuned for the driver's use
 * case. Changes should be carefully bench marked and tested. Comments have been
 * embedded in the source indicating attempted changes and reverts. Due to its
 * position in the driver subtle changes in this class can cause large changes
 * in the performance of the driver.
 * </p>
 *
 * @see <a href="http://code.google.com/p/disruptor/">Disruptor Project</a>
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class PendingMessageQueue {

    /** The mask for constraining the size the message id. */
    public static final long MAX_MESSAGE_ID_MASK = 0x0FFFFFFF;

    /**
     * The maximum size of the queue. This it currently 2^20 but must be at most
     * 2^30 to ensure masking works.
     */
    public static final int MAX_SIZE = (1 << 20);

    /** Amount of time to spin before yielding. Set to 1/100 of a millisecond. */
    public static final long SPIN_TIME_NS = TimeUnit.MILLISECONDS.toNanos(1) / 100;

    /** Amount of time to spin/yield before waiting. Set to 1/2 millisecond. */
    public static final long YIELD_TIME_NS = TimeUnit.MILLISECONDS.toNanos(1) >> 1;

    /** Number of times to spin before trying something different. */
    private static final int SPIN_ITERATIONS = 10000;

    /** The condition used with the queue being full or empty. */
    private final Condition myCondition;

    /** The mutex used with the queue. */
    private final Lock myLock;

    /** The lock type to use with the queue. */
    private final LockType myLockType;

    /** Tracks how many times we have looped through the ring buffer. */
    private final AtomicInteger myLooped;

    /** The mask being used. */
    private final int myMask;

    /** The queue of pending messages. */
    private final PendingMessage[] myQueue;

    /**
     * The position of the last message that is ready to be taken.
     * <p>
     * When ({@link #myReadyBeforePosition} == {@link #myTakePosition}) the
     * queue is empty.
     * </p>
     */
    private final AtomicInteger myReadyBeforePosition;

    /**
     * The position of the next message that can be reserved.
     * <p>
     * When ({@link #myReservePosition} == ({@link #myTakePosition} - 1)) the
     * queue is full.
     * </p>
     */
    private final AtomicInteger myReservePosition;

    /**
     * The position of the next message that can be taken.
     * <p>
     * When ({@link #myReservePosition} == ({@link #myTakePosition} - 1)) the
     * queue is full.
     * </p>
     * <p>
     * When ({@link #myReadyBeforePosition} == {@link #myTakePosition}) the
     * queue is empty.
     * </p>
     */
    private volatile int myTakePosition;

    /** Tracks how many threads are waiting for a message or a space to open. */
    private final AtomicInteger myWaiting;

    /**
     * Creates a new PendingMessageQueue.
     *
     * @param size
     *            The size of the queue to create.
     * @param lockType
     *            The lock type to use with the queue.
     */
    public PendingMessageQueue(final int size, final LockType lockType) {
        int power = size;
        if (MAX_SIZE < size) {
            power = MAX_SIZE;
        }
        else if (Integer.bitCount(size) != 1) {
            // Find the next larger power of 2.
            power = 1;
            while ((power < size) && (power != 0)) {
                power <<= 1;
            }
        }

        myLockType = lockType;
        myQueue = new PendingMessage[power];
        for (int i = 0; i < myQueue.length; ++i) {
            myQueue[i] = new PendingMessage(0, null);
        }
        myMask = (power - 1);

        myLooped = new AtomicInteger(0);
        myTakePosition = -1;
        myReadyBeforePosition = new AtomicInteger(0);
        myReservePosition = new AtomicInteger(0);
        myWaiting = new AtomicInteger(0);

        myLock = new ReentrantLock();
        myCondition = myLock.newCondition();
    }

    /**
     * Returns the size of the queue.
     *
     * @return The size of the queue.
     */
    public int capacity() {
        return myQueue.length - 1;
    }

    /**
     * Drains the list of pending messages into the provided list.
     *
     * @param pending
     *            The list to add all of the pending messages to.
     */
    public void drainTo(final List<PendingMessage> pending) {
        PendingMessage pm = new PendingMessage();
        while (poll(pm)) {
            pending.add(pm);
            pm = new PendingMessage();
        }
    }

    /**
     * Returns true if the queue is empty. e.g., the next take position is the
     * read before position.
     *
     * @return If the queue is empty.
     */
    public boolean isEmpty() {
        final int take = myTakePosition;
        final int readyBefore = myReadyBeforePosition.get();

        return (readyBefore == take) || (take < 0);
    }

    /**
     * Puts a message onto the queue. This method will not block waiting for a
     * space to add the message.
     *
     * @param message
     *            The message to add.
     * @param replyCallback
     *            The callback for the message to add.
     * @return True if the message was added, false otherwise.
     */
    public boolean offer(final Message message,
            final ReplyCallback replyCallback) {

        final int loop = myLooped.get();
        final int reserve = offer();
        if (reserve < 0) {
            return false;
        }

        final int messageid = toMessageId(loop, reserve);

        myQueue[reserve].set(messageid, message, replyCallback);

        markReady(reserve);

        return true;
    }

    /**
     * Puts a message onto the queue. This method will not block waiting for a
     * space to add the message.
     *
     * @param pendingMessage
     *            The message to add.
     * @return True if the message was added, false otherwise.
     */
    public boolean offer(final PendingMessage pendingMessage) {
        final int reserve = offer();
        if (reserve < 0) {
            return false;
        }

        myQueue[reserve].set(pendingMessage);

        markReady(reserve);

        return true;
    }

    /**
     * Returns the next message from the queue without blocking. <blockquote>
     *
     * <pre>
     * <code>
     * PendingMessage pm = new PendingMessage();
     * if( queue.poll(pm) } {
     *    // Handle the message copied into pm.
     * }
     * </code>
     * </pre>
     *
     * </blockquote>
     *
     * @param copyOut
     *            The {@link PendingMessage} to copy the pending message into.
     * @return True if the pending message was updated.
     */
    public boolean poll(final PendingMessage copyOut) {
        boolean result = false;
        final int take = myTakePosition;
        if ((myReadyBeforePosition.get() != take) && (take >= 0)) { // Empty,
            // Not
            // started?
            copyOut.set(myQueue[take]);
            myQueue[take].clear();
            result = true;

            myTakePosition = increment(take);
            notifyWaiters(false);
        }

        return result;
    }

    /**
     * Puts a message onto the queue. This method will block waiting for a space
     * to add the message.
     *
     * @param message
     *            The message to add.
     * @param replyCallback
     *            The callback for the message to add.
     *
     * @throws InterruptedException
     *             If the thread is interrupted while waiting for the message.
     *             If thrown the message will not have been enqueued.
     */
    public void put(final Message message, final ReplyCallback replyCallback)
            throws InterruptedException {

        int loop = myLooped.get();
        int reserve = offer();
        if (reserve < 0) {

            // Spinning here appears to slow things down.

            // Block.
            try {
                myWaiting.incrementAndGet();
                myLock.lock();

                loop = myLooped.get();
                reserve = offer();
                while (reserve < 0) {
                    myCondition.await();
                    loop = myLooped.get();
                    reserve = offer();
                }
            }
            finally {
                myLock.unlock();
                myWaiting.decrementAndGet();
            }
        }

        final int messageid = toMessageId(loop, reserve);

        myQueue[reserve].set(messageid, message, replyCallback);

        markReady(reserve);
    }

    /**
     * Puts two messages onto the queue. This method will block waiting for a
     * space to add the messages but ensures the messages are in sequence in the
     * queue.
     *
     * @param message
     *            The first message to add.
     * @param replyCallback
     *            The callback for the first message to add.
     * @param message2
     *            The second message to add.
     * @param replyCallback2
     *            The callback for the second message to add.
     *
     * @throws InterruptedException
     *             If the thread is interrupted while waiting for the message.
     *             If thrown neither message will have been enqueued.
     */
    public void put(final Message message, final ReplyCallback replyCallback,
            final Message message2, final ReplyCallback replyCallback2)
            throws InterruptedException {
        int loop = myLooped.get();
        int reserve = offer2();
        if (reserve < 0) {

            // Spinning here appears to slow things down.
            try {
                myWaiting.incrementAndGet();
                myLock.lock();

                loop = myLooped.get();
                reserve = offer2();
                while (reserve < 0) {
                    myCondition.await();
                    loop = myLooped.get();
                    reserve = offer2();
                }
            }
            finally {
                myLock.unlock();
                myWaiting.decrementAndGet();
            }
        }

        // Use reserve + 1 for the second message id since it may have looped
        // and then the math does not work out causing messageId2 to be lower
        // than
        // messageId1, which is bad.
        final int messageId1 = toMessageId(loop, reserve);
        final int messageId2 = toMessageId(loop, reserve + 1);

        final int second = increment(reserve);
        myQueue[reserve].set(messageId1, message, replyCallback);
        myQueue[second].set(messageId2, message2, replyCallback2);

        markReady2(reserve);
    }

    /**
     * Puts a message onto the queue. This method will block waiting for a space
     * to add the message.
     *
     * @param pendingMessage
     *            The message to add.
     *
     * @throws InterruptedException
     *             If the thread is interrupted while waiting for the message.
     *             If thrown the message will not have been enqueued.
     */
    public void put(final PendingMessage pendingMessage)
            throws InterruptedException {
        int reserve = offer();
        if (reserve < 0) {

            // Spinning here appears to slow things down.

            try {
                myWaiting.incrementAndGet();
                myLock.lock();

                reserve = offer();
                while (reserve < 0) {
                    myCondition.await();
                    reserve = offer();
                }
            }
            finally {
                myLock.unlock();
                myWaiting.decrementAndGet();
            }
        }

        myQueue[reserve].set(pendingMessage);

        markReady(reserve);
    }

    /**
     * Returns the number of messages in the queue.
     *
     * @return The number of messages in the queue.
     */
    public int size() {
        final int take = myTakePosition;
        final int ready = myReadyBeforePosition.get();

        if (take < 0) {
            return 0;
        }
        else if (take <= ready) {
            return (ready - take);
        }

        return (myQueue.length - take) + ready;
    }

    /**
     * Returns the next message from the queue and will block waiting for a
     * message.
     *
     * @param copyOut
     *            The {@link PendingMessage} to copy the pending message into.
     * @throws InterruptedException
     *             If the thread is interrupted while waiting for the message.
     */
    public void take(final PendingMessage copyOut) throws InterruptedException {
        if (!poll(copyOut)) {

            // Spin/yeild loop.
            if (myLockType == LockType.LOW_LATENCY_SPIN) {
                long now = 0;
                long spinDeadline = 1;
                long yeildDeadline = 1;
                while (now < yeildDeadline) {
                    for (int i = 0; i < SPIN_ITERATIONS; ++i) {
                        if (poll(copyOut)) {
                            return;
                        }
                    }

                    // Pause?
                    now = System.nanoTime();
                    if (spinDeadline == 1) {
                        spinDeadline = now + SPIN_TIME_NS;
                        yeildDeadline = now + YIELD_TIME_NS;
                        // First time free pass.
                    }
                    else {
                        if ((spinDeadline < now) && (now < yeildDeadline)) {
                            Thread.yield();
                        }
                    }
                }
            }

            // Block.
            try {
                myWaiting.incrementAndGet();
                myLock.lock();

                while (!poll(copyOut)) {
                    myCondition.await();
                }
            }
            finally {
                myLock.unlock();
                myWaiting.decrementAndGet();
            }
        }
    }

    /**
     * Increments the index handling roll-over.
     *
     * @param index
     *            The value to increment.
     * @return The incremented value.
     */
    protected int increment(final int index) {
        return ((index + 1) & myMask);
    }

    /**
     * Marks the position as ready by incrementing the ready position to the
     * provided position. This method uses a spin lock assuming any other
     * threads will increment the ready position quickly to the position just
     * before {@code index}.
     *
     * @param index
     *            The index of the ready message.
     */
    protected void markReady(final int index) {
        final int after = increment(index);

        while (!myReadyBeforePosition.compareAndSet(index, after)) {
            // Spinning here slows things down because we know that the other
            // thread should be runnable. Always Yield.
            Thread.yield();
        }

        // Pull take position into the queue.
        if ((index == 0) && (myTakePosition == -1)) {
            myTakePosition = index;
        }

        notifyWaiters(false);
    }

    /**
     * Marks the position and the next position as ready by incrementing the
     * ready position to the provided position + 1. This method uses a spin lock
     * assuming any other threads will increment the ready position quickly to
     * the position just before {@code index}.
     *
     * @param index
     *            The index of the ready message.
     */
    protected void markReady2(final int index) {
        final int after = increment(index);
        final int twoAfter = increment(after);

        while (!myReadyBeforePosition.compareAndSet(index, twoAfter)) {
            // Just keep swimming...
            Thread.yield();
        }

        // Pull take position into the queue.
        if ((index == 0) && (myTakePosition == -1)) {
            myTakePosition = index;
        }

        // If someone is waiting let them know we created two messages.
        notifyWaiters(true);
    }

    /**
     * Notifies the waiting threads that the state of the queue has changed.
     *
     * @param all
     *            If true then all threads will be woken. Otherwise only a
     *            single thread is woken.
     */
    protected void notifyWaiters(final boolean all) {
        if (myWaiting.get() > 0) {
            try {
                myLock.lock();
                if (all) {
                    myCondition.signalAll();
                }
                else {
                    myCondition.signal();
                }
            }
            finally {
                myLock.unlock();
            }
        }
    }

    /**
     * Checks if there is remove for another message. If so returns the index of
     * the message to update. If not return a value less then zero.
     *
     * @return The position of the message that can be updated or a value of
     *         less than zero if the queue is full.
     */
    protected int offer() {
        int result = -1;
        final int reserve = myReservePosition.get();
        final int next = increment(reserve);
        if ((myTakePosition != next) /* Full? */
                && myReservePosition.compareAndSet(reserve, next)) {

            // Got a slot.
            result = reserve;

            // Check if we looped.
            if (next < reserve) {
                myLooped.incrementAndGet();
            }
        }
        return result;
    }

    /**
     * Checks if there is remove for another two message. If so returns the
     * index of the first message to update. If not return a value less then
     * zero.
     *
     * @return The position of the first message that can be updated or a value
     *         of less than zero if the queue is full.
     */
    protected int offer2() {
        int result = -1;
        final int reserve = myReservePosition.get();
        final int first = increment(reserve);
        final int second = increment(first);
        final int take = myTakePosition;
        if ((take != first) && (take != second) /* Full? */
                && myReservePosition.compareAndSet(reserve, second)) {

            // Got two slots. Return the first.
            result = reserve;

            // Check if we looped.
            if (second < reserve) {
                myLooped.incrementAndGet();
            }
        }
        return result;
    }

    /**
     * Computes a new message id based on the current loop and reserve spot in
     * the queue.
     *
     * @param loop
     *            The number of time the queue has looped over the queue.
     * @param reserve
     *            The reserved position in the queue. This can be a virtual
     *            postion.
     * @return The message id to use.
     */
    private int toMessageId(final int loop, final long reserve) {
        final long loopOffset = (((long) loop) * myQueue.length);
        if (loopOffset > MAX_MESSAGE_ID_MASK) {
            myLooped.compareAndSet(loop, 0);
        }
        // Add an extra 1 so the first value is 1 instead of zero.
        return (int) ((loopOffset + reserve) & MAX_MESSAGE_ID_MASK) + 1;
    }
}
