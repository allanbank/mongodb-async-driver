/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.StreamCallback;

/**
 * LambdaCallbackAdapter provides an adapter for the {@link LambdaCallback} to a
 * {@link Callback} or {@link StreamCallback}.
 *
 * @param <V>
 *            The type of the operation's result.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class LambdaCallbackAdapter<V> implements StreamCallback<V> {

    /** The {@link LambdaCallback} to trigger. */
    private final LambdaCallback<V> myLambda;

    /**
     * Creates a new LambdaCallbackAdapter.
     *
     * @param lambda
     *            The {@link LambdaCallback} to trigger.
     */
    public LambdaCallbackAdapter(final LambdaCallback<V> lambda) {
        myLambda = lambda;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link LambdaCallback#accept(Throwable, Object)
     * accept(null, result)} on the wrapped {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void callback(final V result) {
        myLambda.accept(null, result);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link LambdaCallback#accept(Throwable, Object)
     * accept(null, null)} on the wrapped {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void done() {
        myLambda.accept(null, null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link LambdaCallback#accept(Throwable, Object)
     * accept(thrown, null)} on the wrapped {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myLambda.accept(thrown, null);
    }

}
