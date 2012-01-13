/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.allanbank.mongodb.Callback;

/**
 * Implementation of a {@link Callback} and {@link Future} interfaces. Used to
 * convert a {@link Callback} into a {@link Future} for returning to callers.
 * 
 * @param <V>
 *            The type for the set value.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class FutureCallback<V> implements Future<V>, Callback<V> {

    /**
     * Flag tracking if the user has cancelled the future. This does not stop
     * the MongoDB call.
     */
    private boolean myCancelled = false;

    /** The exception thrown by the call. */
    private Throwable myThrown;

    /** The returned value for the callback. */
    private V myValue = null;

    /**
     * Create a new FutureCallback.
     */
    public FutureCallback() {
        myCancelled = false;
        myValue = null;
        myThrown = null;
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
    public synchronized void callback(final V result) {
        myValue = result;
        notifyAll();
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
    public synchronized boolean cancel(final boolean mayInterruptIfRunning) {
        if ((myValue == null) && (myThrown == null) && (myCancelled == false)) {
            myCancelled = true;
            notifyAll();
            return true;
        }
        return false;
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
    public synchronized void exception(final Throwable thrown) {
        myThrown = thrown;
        notifyAll();
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
    public synchronized V get() throws InterruptedException, ExecutionException {
        if (!isDone()) {
            wait();
        }

        if (myThrown != null) {
            throw new ExecutionException(myThrown);
        }

        return myValue;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the value set via the {@link Callback}.
     * </p>
     * 
     * @see Future#get(long, TimeUnit)
     */
    @Override
    public synchronized V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone()) {
            unit.timedWait(this, timeout);
        }

        if (myThrown != null) {
            throw new ExecutionException(myThrown);
        }
        if (!isDone()) {
            throw new TimeoutException();
        }

        return myValue;
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
    public synchronized boolean isCancelled() {
        return myCancelled;
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
    public synchronized boolean isDone() {
        return ((myValue != null) || (myThrown != null) || (myCancelled == true));
    }
}
