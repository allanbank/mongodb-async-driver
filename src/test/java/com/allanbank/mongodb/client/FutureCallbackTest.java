/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.allanbank.mongodb.LockType;

/**
 * FutureCallbackTest provides tests for the {@link FutureCallback} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FutureCallbackTest {

    /**
     * Test method for {@link FutureCallback#addListener}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testAddListener() throws Exception {

        final Runnable mockToRun = createMock(Runnable.class);
        final Executor mockExecutor = createMock(Executor.class);

        mockExecutor.execute(mockToRun);
        expectLastCall();

        replay(mockToRun, mockExecutor);

        final FutureCallback<Object> callback = new FutureCallback<Object>();

        callback.addListener(mockToRun, mockExecutor);

        final Object result = new Object();
        callback.callback(result);

        assertSame(result, callback.get());
        assertSame(result, callback.get(1, TimeUnit.NANOSECONDS));

        verify(mockToRun, mockExecutor);
    }

    /**
     * Test method for {@link FutureCallback#addListener}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testAddListenerExecuteThrows() throws Exception {

        final Runnable mockToRun = createMock(Runnable.class);
        final Executor mockExecutor = createMock(Executor.class);

        mockExecutor.execute(mockToRun);
        expectLastCall().andThrow(new RejectedExecutionException("Injected"));

        replay(mockToRun, mockExecutor);

        final FutureCallback<Object> callback = new FutureCallback<Object>();

        final Object result = new Object();
        callback.callback(result);

        assertSame(result, callback.get());
        assertSame(result, callback.get(1, TimeUnit.NANOSECONDS));

        callback.addListener(mockToRun, mockExecutor);

        verify(mockToRun, mockExecutor);
    }

    /**
     * Test method for {@link FutureCallback#addListener}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testAddListenerPostCallback() throws Exception {

        final Runnable mockToRun = createMock(Runnable.class);
        final Executor mockExecutor = createMock(Executor.class);

        mockExecutor.execute(mockToRun);
        expectLastCall();

        replay(mockToRun, mockExecutor);

        final FutureCallback<Object> callback = new FutureCallback<Object>();

        final Object result = new Object();
        callback.callback(result);

        assertSame(result, callback.get());
        assertSame(result, callback.get(1, TimeUnit.NANOSECONDS));

        callback.addListener(mockToRun, mockExecutor);

        verify(mockToRun, mockExecutor);
    }

    /**
     * Test method for {@link FutureCallback#addListener}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testAddListenerRaces() throws Exception {
        final FutureCallback<Object> callback = new FutureCallback<Object>();
        final Object result = new Object();

        final Executor executor = new Executor() {

            @Override
            public void execute(final Runnable command) {
                command.run();
            }
        };

        final Thread[] threads = new Thread[101];
        final Runnable[] mocksToRun = new Runnable[threads.length - 1];
        for (int i = 0; i < mocksToRun.length; ++i) {
            mocksToRun[i] = createMock(Runnable.class);
            mocksToRun[i].run();
            expectLastCall();
            replay(mocksToRun[i]);

            final Runnable toRun = mocksToRun[i];
            threads[i] = new Thread() {
                @Override
                public void run() {
                    callback.addListener(toRun, executor);
                }
            };
        }
        threads[threads.length - 1] = new Thread() {
            @Override
            public void run() {
                callback.callback(result);
            }
        };

        // Shuffle the execute order.
        Collections.shuffle(Arrays.asList(threads));

        // Run the Threads.
        for (final Thread t : threads) {
            t.start();
        }

        // Wait for the threads.
        for (final Thread t : threads) {
            t.join();
        }

        for (final Runnable mockRunnable : mocksToRun) {
            verify(mockRunnable);
        }
    }

    /**
     * Test method for {@link FutureCallback#callback(Object)}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testCallback() throws Exception {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        final Object result = new Object();
        callback.callback(result);

        assertSame(result, callback.get());
        assertSame(result, callback.get(1, TimeUnit.NANOSECONDS));
    }

    /**
     * Test method for {@link FutureCallback#callback(Object)}.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testCallbackWithNull() throws Exception {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        callback.callback(null);

        assertNull(callback.get());
        assertNull(callback.get(1, TimeUnit.NANOSECONDS));
    }

    /**
     * Test method for {@link FutureCallback#cancel(boolean)}.
     */
    @Test
    public void testCancel() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        assertTrue(callback.cancel(false));
        assertTrue(callback.isCancelled());

        try {
            callback.get();
            fail("Should have thrown an IntterruptedException.");
        }
        catch (final CancellationException e) {
            // Good.
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }

        try {
            callback.get(1, TimeUnit.NANOSECONDS);
            fail("Should have thrown an IntterruptedException.");
        }
        catch (final CancellationException e) {
            // Good.
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final TimeoutException te) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(te);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link FutureCallback#cancel(boolean)}.
     */
    @Test
    public void testCancelFails() {

        FutureCallback<Object> callback;

        callback = new FutureCallback<Object>();
        callback.callback(new Object());
        assertFalse(callback.cancel(false));
        assertFalse(callback.isCancelled());

        callback = new FutureCallback<Object>();
        callback.exception(new Throwable());
        assertFalse(callback.cancel(false));
        assertFalse(callback.isCancelled());

        callback = new FutureCallback<Object>();
        assertTrue(callback.cancel(false));
        assertFalse(callback.cancel(false));
        assertTrue(callback.isCancelled());
    }

    /**
     * Test method for {@link FutureCallback#cancel(boolean)}.
     */
    @Test
    public void testCancelInterrupt() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        assertTrue(callback.cancel(true));
        assertTrue(callback.isCancelled());

        try {
            callback.get();
            fail("Should have thrown an IntterruptedException.");
        }
        catch (final CancellationException e) {
            // Good.
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }

        try {
            callback.get(1, TimeUnit.NANOSECONDS);
            fail("Should have thrown an IntterruptedException.");
        }
        catch (final CancellationException e) {
            // Good.
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final TimeoutException te) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(te);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link FutureCallback#cancel(boolean)} .
     */
    @Test
    public void testConcurrentCancel() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        final AtomicInteger failCount = new AtomicInteger(0);
        final AtomicInteger successCount = new AtomicInteger(0);

        final Thread[] threads = new Thread[100];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    Thread.yield();
                    if (callback.cancel(false)) {
                        successCount.incrementAndGet();
                    }
                    else {
                        failCount.incrementAndGet();
                    }
                }
            };
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            try {
                t.join();
            }
            catch (final InterruptedException e) {
                fail(e.getMessage());
            }
        }

        assertThat(successCount.get(), is(1));
        assertThat(failCount.get(), is(threads.length - 1));
    }

    /**
     * Test method for {@link FutureCallback#exception(Throwable)} .
     */
    @Test
    public void testException() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        final Throwable thrown = new Throwable();
        callback.exception(thrown);

        try {
            callback.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            // Good.
            assertSame(thrown, e.getCause());
        }

        try {
            callback.get(1, TimeUnit.NANOSECONDS);
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
        catch (final TimeoutException te) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(te);
            throw error;
        }
        catch (final ExecutionException e) {
            // Good
            assertSame(thrown, e.getCause());
        }
    }

    /**
     * Test method for {@link FutureCallback#get()}.
     */
    @Test
    public void testGetFromAnotherThread() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();
        final Object result = new Object();

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50);
                }
                catch (final InterruptedException e) {
                    // Ignore
                }
                finally {
                    callback.callback(result);
                }
            }
        });

        t.start();
        try {
            assertSame(result, callback.get());
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have been interrupted.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link FutureCallback#get()}.
     */
    @Test
    public void testGetFromAnotherThreadWithSpin() {
        final FutureCallback<Object> callback = new FutureCallback<Object>(
                LockType.LOW_LATENCY_SPIN);
        final Object result = new Object();

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50);
                }
                catch (final InterruptedException e) {
                    // Ignore
                }
                finally {
                    callback.callback(result);
                }
            }
        });

        t.start();
        try {
            assertSame(result, callback.get());
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have been interrupted.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link FutureCallback#get()}.
     */
    @Test
    public void testGetFromAnotherThreadWithSpinSleepNanos() {
        final FutureCallback<Object> callback = new FutureCallback<Object>(
                LockType.LOW_LATENCY_SPIN);
        final Object result = new Object();

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(0, 5);
                }
                catch (final InterruptedException e) {
                    // Ignore
                }
                finally {
                    callback.callback(result);
                }
            }
        });

        t.start();
        try {
            assertSame(result, callback.get());
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have been interrupted.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link FutureCallback#get()}.
     */
    @Test
    public void testGetTimesOut() {
        final FutureCallback<Object> callback = new FutureCallback<Object>();

        try {
            callback.get(10, TimeUnit.MILLISECONDS);
        }
        catch (final TimeoutException good) {
            // Good.
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(
                    "Should not have been interrupted.");
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(
                    "Should not have seen an error.");
            error.initCause(e);
            throw error;
        }
    }

}
