/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private volatile boolean myCancelled = false;

    /**
     * Flag tracking if the value has been set.
     */
    private volatile boolean mySet = false;

    /** Lock to handle conditioned waits. */
    private final Lock myLock;

    /** Condition to wait on for a result. */
    private final Condition myNoResultCondition;

    /** The exception thrown by the call. */
    private volatile Throwable myThrown;

    /** The returned value for the callback. */
    private volatile V myValue = null;

    /**
     * Create a new FutureCallback.
     */
    public FutureCallback() {
        myCancelled = false;
        myValue = null;
        myThrown = null;

        myLock = new ReentrantLock();
        myNoResultCondition = myLock.newCondition();
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
        myLock.lock();
        try {
            myValue = result;
            mySet = true;
            myNoResultCondition.signalAll();
        }
        finally {
            myLock.unlock();
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
        if ((myValue == null) && (myThrown == null) && (myCancelled == false)) {
            myLock.lock();
            try {
                myCancelled = true;
                myNoResultCondition.signalAll();
            }
            finally {
                myLock.unlock();
            }
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
    public void exception(final Throwable thrown) {
        myLock.lock();
        try {
            myThrown = thrown;
            mySet = true;
            myNoResultCondition.signalAll();
        }
        finally {
            myLock.unlock();
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
        myLock.lock();
        try {
            if (!isDone()) {
                myNoResultCondition.await();
            }
        }
        finally {
            myLock.unlock();
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
    public V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        boolean done = false;
        myLock.lock();
        try {
            if (!isDone()) {
                myNoResultCondition.await(timeout, unit);
                done = isDone();
            }
            else {
                done = true;
            }
        }
        finally {
            myLock.unlock();
        }

        if (myThrown != null) {
            throw new ExecutionException(myThrown);
        }
        if (!done) {
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
    public boolean isCancelled() {
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
    public boolean isDone() {
        return (mySet || myCancelled);
    }
}
