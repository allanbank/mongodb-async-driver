/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.connection.Message;

/**
 * PendingMessageQueueTest provides tests for the {@link PendingMessageQueue}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PendingMessageQueueTest {

    /**
     * Test method for {@link PendingMessageQueue#drainTo(List)} .
     */
    @Test
    public void testDrainTo() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < queue.capacity(); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
                assertEquals(i + 1, queue.size());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        assertFalse(queue.isEmpty());
        final List<PendingMessage> drained = new ArrayList<PendingMessage>();
        queue.drainTo(drained);
        assertTrue(queue.isEmpty());

        assertEquals(queue.capacity(), drained.size());
        assertEquals(0, queue.size());

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {
            final PendingMessage pm = drained.get(i);
            assertNotNull("" + i, pm);
            assertEquals(i + 1, pm.getMessageId());
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#drainTo(List)} .
     */
    @Test
    public void testDrainToWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < queue.capacity(); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
                assertEquals(i + 1, queue.size());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        assertFalse(queue.isEmpty());
        final List<PendingMessage> drained = new ArrayList<PendingMessage>();
        queue.drainTo(drained);
        assertTrue(queue.isEmpty());

        assertEquals(queue.capacity(), drained.size());
        assertEquals(0, queue.size());

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {
            final PendingMessage pm = drained.get(i);
            assertNotNull("" + i, pm);
            assertEquals(i + 1, pm.getMessageId());
        }
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testMultipleFastProducerFastConsumer()
            throws InterruptedException {

        final int count = 5000;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.MUTEX);

        final FastProducer[] producer = new FastProducer[20];
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread[] pThread = new Thread[producer.length];
        final Thread cThread = new Thread(consumer);

        cThread.start();

        for (int i = 0; i < producer.length; ++i) {
            producer[i] = new FastProducer(queue, count);
            pThread[i] = new Thread(producer[i]);
            pThread[i].start();
        }

        assertTrue(cThread.isAlive());

        for (int i = 0; i < producer.length; ++i) {
            pThread[i].join(TimeUnit.SECONDS.toMillis(30));
        }

        for (int i = 0; i < producer.length; ++i) {
            assertFalse(pThread[i].isAlive());
        }

        waitForDone(count * producer.length, queue, consumer);

        assertEquals(count * producer.length, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testMultipleFastProducerFastConsumerWithLowLatencyLock()
            throws InterruptedException {

        final int count = 5000;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.LOW_LATENCY_SPIN);

        final FastProducer[] producer = new FastProducer[20];
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread[] pThread = new Thread[producer.length];
        final Thread cThread = new Thread(consumer);

        cThread.start();

        for (int i = 0; i < producer.length; ++i) {
            producer[i] = new FastProducer(queue, count);
            pThread[i] = new Thread(producer[i]);
            pThread[i].start();
        }

        assertTrue(cThread.isAlive());

        for (int i = 0; i < producer.length; ++i) {
            pThread[i].join(TimeUnit.SECONDS.toMillis(30));
        }

        for (int i = 0; i < producer.length; ++i) {
            assertFalse(pThread[i].isAlive());
        }

        waitForDone(count * producer.length, queue, consumer);

        assertEquals(count * producer.length, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testMultipleSlowProducerFastConsumer()
            throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.MUTEX);

        final SlowProducer[] producer = new SlowProducer[20];
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread[] pThread = new Thread[producer.length];
        final Thread cThread = new Thread(consumer);

        cThread.start();

        for (int i = 0; i < producer.length; ++i) {
            producer[i] = new SlowProducer(queue, count);
            pThread[i] = new Thread(producer[i]);
            pThread[i].start();
        }

        assertTrue(cThread.isAlive());
        for (int i = 0; i < producer.length; ++i) {
            assertTrue(pThread[i].isAlive());
        }

        for (int i = 0; i < producer.length; ++i) {
            pThread[i].join(TimeUnit.SECONDS.toMillis(30));
        }

        for (int i = 0; i < producer.length; ++i) {
            assertFalse(pThread[i].isAlive());
        }

        waitForDone(count * producer.length, queue, consumer);

        assertEquals(count * producer.length, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testMultipleSlowProducerFastConsumerWithLowLatencyLock()
            throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.LOW_LATENCY_SPIN);

        final SlowProducer[] producer = new SlowProducer[20];
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread[] pThread = new Thread[producer.length];
        final Thread cThread = new Thread(consumer);

        cThread.start();

        for (int i = 0; i < producer.length; ++i) {
            producer[i] = new SlowProducer(queue, count);
            pThread[i] = new Thread(producer[i]);
            pThread[i].start();
        }

        assertTrue(cThread.isAlive());
        for (int i = 0; i < producer.length; ++i) {
            assertTrue(pThread[i].isAlive());
        }

        for (int i = 0; i < producer.length; ++i) {
            pThread[i].join(TimeUnit.SECONDS.toMillis(30));
        }

        for (int i = 0; i < producer.length; ++i) {
            assertFalse(pThread[i].isAlive());
        }

        waitForDone(count * producer.length, queue, consumer);

        assertEquals(count * producer.length, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for {@link PendingMessageQueue#offer(PendingMessage)} .
     */
    @Test
    public void testOfferPendingMessage() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {

            assertTrue(queue.offer(new PendingMessage(i, null)));

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#offer(PendingMessage)} .
     */
    @Test
    public void testOfferPendingMessageWhenFull() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            assertTrue(queue.offer(new PendingMessage(i, null)));
        }

        assertFalse(queue.offer(new PendingMessage(9999, null)));
    }

    /**
     * Test method for {@link PendingMessageQueue#offer(PendingMessage)} .
     */
    @Test
    public void testOfferPendingMessageWhenFullWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            assertTrue(queue.offer(new PendingMessage(i, null)));
        }

        assertFalse(queue.offer(new PendingMessage(9999, null)));
    }

    /**
     * Test method for {@link PendingMessageQueue#offer(PendingMessage)} .
     */
    @Test
    public void testOfferPendingMessageWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {

            assertTrue(queue.offer(new PendingMessage(i, null)));

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueue() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        assertEquals(1023, queue.capacity());
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueueNonPowerOfTwo() {
        final PendingMessageQueue queue = new PendingMessageQueue(1022,
                LockType.MUTEX);
        assertEquals(1023, queue.capacity());
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueueNonPowerOfTwoWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1022,
                LockType.LOW_LATENCY_SPIN);
        assertEquals(1023, queue.capacity());
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueueTooBig() {
        final PendingMessageQueue queue = new PendingMessageQueue(
                PendingMessageQueue.MAX_SIZE + 1, LockType.MUTEX);
        assertEquals(PendingMessageQueue.MAX_SIZE - 1, queue.capacity());
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueueTooBigWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(
                PendingMessageQueue.MAX_SIZE + 1, LockType.LOW_LATENCY_SPIN);
        assertEquals(PendingMessageQueue.MAX_SIZE - 1, queue.capacity());
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#PendingMessageQueue(int, LockType)}.
     */
    @Test
    public void testPendingMessageQueueWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        assertEquals(1023, queue.capacity());
    }

    /**
     * Test method for {@link PendingMessageQueue#poll} .
     */
    @Test
    public void testPoll() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        final PendingMessage pm = new PendingMessage();

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {

            assertFalse(queue.isEmpty());

            assertTrue(queue.poll(pm));

            assertNotNull("" + i, pm);
            assertEquals(i + 1, pm.getMessageId());
        }

        // Another poll should return false.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(pm));

        //
        // Make sure everything works over a roll over.
        //

        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {

            assertFalse(queue.isEmpty());

            assertTrue(queue.poll(pm));

            assertNotNull("" + i, pm);
            assertEquals(i + queue.capacity() + 1, pm.getMessageId());
        }

        // Another poll should return false.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(pm));
    }

    /**
     * Test method for {@link PendingMessageQueue#poll} .
     */
    @Test
    public void testPollNotStarted() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);

        // A poll should return null.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(new PendingMessage()));
    }

    /**
     * Test method for {@link PendingMessageQueue#poll} .
     */
    @Test
    public void testPollNotStartedWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);

        // A poll should return null.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(new PendingMessage()));
    }

    /**
     * Test method for {@link PendingMessageQueue#poll} .
     */
    @Test
    public void testPollWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        final PendingMessage pm = new PendingMessage();

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {

            assertFalse(queue.isEmpty());

            assertTrue(queue.poll(pm));

            assertNotNull("" + i, pm);
            assertEquals(i + 1, pm.getMessageId());
        }

        // Another poll should return false.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(pm));

        //
        // Make sure everything works over a roll over.
        //

        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
                assertFalse(queue.isEmpty());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        // Now poll for all of them,
        for (int i = 0; i < (queue.capacity()); ++i) {

            assertFalse(queue.isEmpty());

            assertTrue(queue.poll(pm));

            assertNotNull("" + i, pm);
            assertEquals(i + queue.capacity() + 1, pm.getMessageId());
        }

        // Another poll should return false.
        assertTrue(queue.isEmpty());
        assertFalse(queue.poll(pm));
    }

    /**
     * Test method for {@link PendingMessageQueue#put(Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReply() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#put(Message, Callback, Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyIntMessageCallbackOfReply() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity() * 3); i += 2) {
            try {
                queue.put(null, null, null, null);
                assertEquals("" + i, 2, queue.size());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            assertEquals("" + i, 1, queue.size());

            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 2, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
            assertEquals("" + i, 0, queue.size());
        }
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#put(Message, Callback, Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyIntMessageCallbackOfReplyWhenFull() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity() - 1); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(null, null, null, null);
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#put(Message, Callback, Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyIntMessageCallbackOfReplyWhenFullWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity() - 1); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(null, null, null, null);
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for
     * {@link PendingMessageQueue#put(Message, Callback, Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyIntMessageCallbackOfReplyWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity() * 3); i += 2) {
            try {
                queue.put(null, null, null, null);
                assertEquals("" + i, 2, queue.size());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            assertEquals("" + i, 1, queue.size());

            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 2, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
            assertEquals("" + i, 0, queue.size());
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyWhenFull() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(null, null);
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyWhenFullWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(null, null);
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(Message, Callback)} .
     */
    @Test
    public void testPutIntMessageCallbackOfReplyWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(PendingMessage)} .
     */
    @Test
    public void testPutPendingMessage() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(new PendingMessage(i, null));
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(PendingMessage)} .
     */
    @Test
    public void testPutPendingMessageWhenFull() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(new PendingMessage(i, null));
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(new PendingMessage(9999, null));
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(PendingMessage)} .
     */
    @Test
    public void testPutPendingMessageWhenFullWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        // Fill the queue.
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(new PendingMessage(i, null));
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        try {
            Thread.currentThread().interrupt();
            queue.put(new PendingMessage(9999, null));
            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good.
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#put(PendingMessage)} .
     */
    @Test
    public void testPutPendingMessageWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(new PendingMessage(i, null));
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleProducerFastConsumer() throws InterruptedException {

        final int count = 100000;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.MUTEX);

        final FastProducer producer = new FastProducer(queue, count);
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleProducerFastConsumerWithLowLatencyLock()
            throws InterruptedException {

        final int count = 100000;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.LOW_LATENCY_SPIN);

        final FastProducer producer = new FastProducer(queue, count);
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleProducerSlowConsumer() throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.MUTEX);

        final FastProducer producer = new FastProducer(queue, count);
        final SlowConsumer consumer = new SlowConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleProducerSlowConsumerWithLowLatencyLock()
            throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.LOW_LATENCY_SPIN);

        final FastProducer producer = new FastProducer(queue, count);
        final SlowConsumer consumer = new SlowConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleSlowProducerFastConsumer()
            throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.MUTEX);

        final SlowProducer producer = new SlowProducer(queue, count);
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for a single producer and slow consumer.
     * 
     * @throws InterruptedException
     *             On a failure waiting for the test.
     */
    @Test
    public void testSingleSlowProducerFastConsumerWithLowLatencyLock()
            throws InterruptedException {

        final int count = 1024;

        final PendingMessageQueue queue = new PendingMessageQueue(256,
                LockType.LOW_LATENCY_SPIN);

        final SlowProducer producer = new SlowProducer(queue, count);
        final FastConsumer consumer = new FastConsumer(queue);

        final Thread pThread = new Thread(producer);
        final Thread cThread = new Thread(consumer);

        cThread.start();
        pThread.start();

        assertTrue(cThread.isAlive());
        assertTrue(pThread.isAlive());

        pThread.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse(pThread.isAlive());

        waitForDone(count, queue, consumer);

        assertEquals(count, consumer.getCount());
        cThread.interrupt();
        cThread.join(TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTake() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull("" + i, pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTakeExhuasted() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        final PendingMessage pm = new PendingMessage();
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        Thread.currentThread().interrupt();
        try {
            queue.take(pm);

            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTakeExhuastedWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        final PendingMessage pm = new PendingMessage();
        for (int i = 0; i < (queue.capacity()); ++i) {
            try {
                queue.take(pm);

                assertNotNull(pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }

        Thread.currentThread().interrupt();
        try {
            queue.take(pm);

            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTakeNotStarted() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.MUTEX);

        Thread.currentThread().interrupt();
        final PendingMessage pm = new PendingMessage();
        try {
            queue.take(pm);

            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTakeNotStartedWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);

        Thread.currentThread().interrupt();
        final PendingMessage pm = new PendingMessage();
        try {
            queue.take(pm);

            fail("Should have tried to wait() and thrown an InterruptedException.");
        }
        catch (final InterruptedException ie) {
            // Good
        }
    }

    /**
     * Test method for {@link PendingMessageQueue#take} .
     */
    @Test
    public void testTakeWithLowLatencyLock() {
        final PendingMessageQueue queue = new PendingMessageQueue(1024,
                LockType.LOW_LATENCY_SPIN);
        for (int i = 0; i < (queue.capacity() * 3); ++i) {
            try {
                queue.put(null, null);
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }

            final PendingMessage pm = new PendingMessage();
            try {
                queue.take(pm);

                assertNotNull("" + i, pm);
                assertEquals(i + 1, pm.getMessageId());
            }
            catch (final InterruptedException ie) {
                fail(ie.getMessage());
            }
        }
    }

    /**
     * Waits for the queue to empty.
     * 
     * @param count
     *            The expected count.
     * @param queue
     *            The queue to be emptied.
     * @param consumer
     *            The consumer reading.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    protected void waitForDone(final int count,
            final PendingMessageQueue queue, final FastConsumer consumer)
            throws InterruptedException {
        final long start = System.currentTimeMillis();
        final long deadline = start + TimeUnit.SECONDS.toMillis(30);
        while ((start < deadline) && !queue.isEmpty()
                && (consumer.getCount() < count)) {
            Thread.sleep(5);
        }
    }

    /**
     * Waits for the queue to empty.
     * 
     * @param count
     *            The expected count.
     * @param queue
     *            The queue to be emptied.
     * @param consumer
     *            The consumer reading.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    protected void waitForDone(final int count,
            final PendingMessageQueue queue, final SlowConsumer consumer)
            throws InterruptedException {
        final long start = System.currentTimeMillis();
        final long deadline = start + TimeUnit.SECONDS.toMillis(30);
        while ((start < deadline) && !queue.isEmpty()
                && (consumer.getCount() < count)) {
            Thread.sleep(5);
        }
    }

    /**
     * FastConsumer provides a fast consumer of messages.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final static class FastConsumer implements Runnable {

        /** The number of messages consumed. */
        private int myCount = 0;

        /** The queue to consume from. */
        private final PendingMessageQueue myQueue;

        /**
         * Creates a new FastConsumer.
         * 
         * @param queue
         *            The queue to consume from.
         */
        public FastConsumer(final PendingMessageQueue queue) {
            myQueue = queue;
        }

        /**
         * Returns the count value.
         * 
         * @return The count value.
         */
        public int getCount() {
            return myCount;
        }

        @Override
        public void run() {
            try {
                final PendingMessage message = new PendingMessage();
                while (true) {
                    myQueue.take(message);
                    myCount += 1;
                }
            }
            catch (final InterruptedException ie) {
                // Nothing.
            }
        }
    }

    /**
     * FastProducer provides a fast producer of messages.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final static class FastProducer implements Runnable {

        /** The number of messages consumed. */
        private final int myCount;

        /** The queue to write to. */
        private final PendingMessageQueue myQueue;

        /**
         * Creates a new FastProducer.
         * 
         * @param queue
         *            The queue to write to.
         * @param count
         *            The number of messages to produce.
         */
        public FastProducer(final PendingMessageQueue queue, final int count) {
            myQueue = queue;
            myCount = count;
        }

        @Override
        public void run() {
            try {
                final PendingMessage message = new PendingMessage();
                for (int i = 0; i < myCount; ++i) {
                    message.set(i, null, null);
                    myQueue.put(message);
                }
            }
            catch (final InterruptedException ie) {
                // Nothing.
            }
        }
    }

    /**
     * SlowConsumer provides a slow consumer of messages.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final static class SlowConsumer implements Runnable {

        /** The number of messages consumed. */
        private int myCount = 0;

        /** The queue to consume from. */
        private final PendingMessageQueue myQueue;

        /**
         * Creates a new SlowConsumer.
         * 
         * @param queue
         *            The queue to consume from.
         */
        public SlowConsumer(final PendingMessageQueue queue) {
            myQueue = queue;
        }

        /**
         * Returns the count value.
         * 
         * @return The count value.
         */
        public int getCount() {
            return myCount;
        }

        @Override
        public void run() {
            try {
                final PendingMessage message = new PendingMessage();
                while (true) {
                    myQueue.take(message);
                    myCount += 1;

                    // Slow.
                    Thread.sleep(1);
                }
            }
            catch (final InterruptedException ie) {
                // Nothing.
            }
        }
    }

    /**
     * SlowProducer provides a slow producer of messages.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final static class SlowProducer implements Runnable {

        /** The number of messages consumed. */
        private final int myCount;

        /** The queue to write to. */
        private final PendingMessageQueue myQueue;

        /**
         * Creates a new SlowProducer.
         * 
         * @param queue
         *            The queue to write to.
         * @param count
         *            The number of messages to produce.
         */
        public SlowProducer(final PendingMessageQueue queue, final int count) {
            myQueue = queue;
            myCount = count;
        }

        @Override
        public void run() {
            try {
                final PendingMessage message = new PendingMessage();
                for (int i = 0; i < myCount; ++i) {
                    message.set(i, null, null);
                    myQueue.put(message);

                    // Slow.
                    if (Math.random() < 0.10) {
                        Thread.sleep(3);
                    }
                    else {
                        Thread.sleep(1);
                    }
                }
            }
            catch (final InterruptedException ie) {
                // Nothing.
            }
        }
    }

}
