/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

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

        final Thread[] threads = new Thread[1000];
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
        assertThat(sequence.noWaiter(1000), is(true));
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
}
