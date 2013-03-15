/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;

/**
 * LongToIntCallback provides a simple callback wrapper to convert the long
 * value to an integer.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LongToIntCallback implements Callback<Long> {

    /** The delegate callback to forward the integer result to. */
    private final Callback<Integer> myDelegate;

    /**
     * Creates a new LongToIntCallback.
     * 
     * @param delegate
     *            The delegate callback to forward the integer result to.
     */
    public LongToIntCallback(final Callback<Integer> delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to cast the Long to an integer and foward to the delegate
     * callback.
     * </p>
     */
    @Override
    public void callback(final Long result) {
        myDelegate.callback(Integer.valueOf(result.intValue()));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the exception to the delegate callback.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myDelegate.exception(thrown);
    }
}
