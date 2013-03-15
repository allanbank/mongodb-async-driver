/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

/**
 * FutureCallbackTest provides tests for the {@link FutureCallback} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FutureCallbackTest {

    /**
     * Test method for {@link FutureCallback#callback(Object)} .
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
     * Test method for {@link FutureCallback#callback(Object)} .
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
        catch (final InterruptedException e) {
            // Good.
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
        catch (final InterruptedException e) {
            // Good.
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
