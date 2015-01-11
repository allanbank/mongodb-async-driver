/*
 * #%L
 * SequenceTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.LockType;

/**
 * SequenceTest provides tests for the {@link Sequence} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SequenceTest {

    /**
     * Test method for {@link Sequence#release(long, long)}.
     *
     * @throws InterruptedException
     *             On a failure to wait for the test threads to finish.
     */
    @Test
    public void testConcurrentSequence() throws InterruptedException {
        final Sequence sequence = new Sequence(0);

        final Runnable runMe = new Runnable() {

            @Override
            public void run() {
                final long value = sequence.reserve(1);
                Thread.yield();
                if (Math.random() < 0.50) {
                    sequence.waitFor(value);
                    Thread.yield();
                }
                sequence.release(value, value + 1);
            }
        };

        final int threadCount = 100;
        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(runMe);
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            t.join();
        }

        assertThat(sequence.isIdle(), is(true));
        assertThat(sequence.noWaiter(threadCount), is(true));
    }

    /**
     * Test method for {@link Sequence#isIdle()}.
     */
    @Test
    public void testIsIdle() {
        final Sequence sequence = new Sequence(0);

        assertThat(sequence.isIdle(), is(true));

        assertThat(sequence.reserve(1), is(0L));
        assertThat(sequence.isIdle(), is(false));

        sequence.waitFor(0);
        assertThat(sequence.isIdle(), is(false));

        sequence.release(0, 1);
        assertThat(sequence.isIdle(), is(true));
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testNoWaiter() {
        final Sequence sequence = new Sequence(0);

        assertThat(sequence.noWaiter(0), is(true));

        assertThat(sequence.reserve(1), is(0L));
        assertThat(sequence.noWaiter(0), is(false));
        assertThat(sequence.noWaiter(1), is(true));

        sequence.waitFor(0);
        assertThat(sequence.noWaiter(1), is(true));

        sequence.release(0, 1);
        assertThat(sequence.noWaiter(1), is(true));
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreaded() {
        final Sequence sequence = new Sequence(0);

        final int iterations = 100;
        final int threadCount = 100;

        doThreadedRun(sequence, iterations, 100, threadCount);
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreadedLightContention() {
        final Sequence sequence = new Sequence(0);

        final int iterations = 100;
        final int threadCount = 2;

        doThreadedRun(sequence, iterations, 0, threadCount);
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreadedMediumContention() {
        final Sequence sequence = new Sequence(0);

        final int iterations = 100;
        final int threadCount = 5;

        doThreadedRun(sequence, iterations, 5, threadCount);
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreadedSpinLock() {
        final Sequence sequence = new Sequence(0, LockType.LOW_LATENCY_SPIN);

        final int iterations = 100;
        final int threadCount = 100;

        doThreadedRun(sequence, iterations, 100, threadCount);
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreadedSpinLockLightContention() {
        final Sequence sequence = new Sequence(0, LockType.LOW_LATENCY_SPIN);

        final int iterations = 100;
        final int threadCount = 2;

        doThreadedRun(sequence, iterations, 0, threadCount);
    }

    /**
     * Test method for {@link Sequence#noWaiter(long)}.
     */
    @Test
    public void testThreadedSpinLockMediumContention() {
        final Sequence sequence = new Sequence(0, LockType.LOW_LATENCY_SPIN);

        final int iterations = 100;
        final int threadCount = 5;

        doThreadedRun(sequence, iterations, 5, threadCount);
    }

    /**
     * Runs a threaded test with the sequence, iterations and thread count.
     *
     * @param sequence
     *            The sequence to test.
     * @param iterations
     *            The iterations for the test.
     * @param piSlices
     *            The number of iterations in computing Pi.
     * @param threadCount
     *            The threads to use.
     */
    private void doThreadedRun(final Sequence sequence, final int iterations,
            final int piSlices, final int threadCount) {
        final CountDownLatch latch = new CountDownLatch(1);
        final Runner[] runners = new Runner[threadCount];
        final Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; ++i) {
            runners[i] = new Runner(iterations, piSlices, sequence, latch);
            threads[i] = new Thread(runners[i]);

            threads[i].start();
        }

        latch.countDown();

        for (int i = 0; i < threadCount; ++i) {
            try {
                threads[i].join(TimeUnit.MINUTES.toMillis(1));

                assertThat(runners[i].getError(), nullValue());
            }
            catch (final InterruptedException e) {
                assertThat(e, nullValue());
            }
        }
    }

    /**
     * Runner provides a class to run the sequence.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class Runner
            implements Runnable {

        /** Set to the exception thrown. */
        private Throwable myError;

        /** The number of iterations to run. */
        private final int myIterations;

        /** The latch we are testing. */
        private final CountDownLatch myLatch;

        /** The number of iterations in computing Pi. */
        private final int myPiSlices;

        /** The sequence we are testing. */
        private final Sequence mySequence;

        /**
         * Creates a new Runner.
         *
         * @param iterations
         *            The number of iterations to run.
         * @param piSlices
         *            The number of iterations in computing Pi.
         * @param sequence
         *            The sequence we are testing.
         * @param latch
         *            The latch we are testing.
         */
        protected Runner(final int iterations, final int piSlices,
                final Sequence sequence, final CountDownLatch latch) {
            super();
            myIterations = iterations;
            myPiSlices = piSlices;
            mySequence = sequence;
            myLatch = latch;
        }

        /**
         * Returns the error value.
         *
         * @return The error value.
         */
        public Throwable getError() {
            return myError;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            final Random rand = new Random(System.currentTimeMillis());

            double pi = 0;
            try {
                myLatch.await();
                for (int i = 0; i < myIterations; ++i) {
                    final int count = rand.nextBoolean() ? 2 : 1;

                    final long reserved = mySequence.reserve(count);
                    mySequence.waitFor(reserved);

                    // Compute pi. Yum.
                    pi = 0;
                    for (int j = 1; j < myPiSlices; j += 2) {
                        pi += ((1.0 / ((2.0 * j) - 1)) - (1.0 / ((2.0 * j) + 1)));
                    }
                    pi *= 4.0;

                    mySequence.release(reserved, reserved + count);
                }
            }
            catch (final InterruptedException e) {
                myError = e;
            }
            catch (final RuntimeException e) {
                myError = e;
            }

            // So the compiler/Jit does not remove the compute of Pi.
            rand.setSeed((long) pi);
        }
    }
}
