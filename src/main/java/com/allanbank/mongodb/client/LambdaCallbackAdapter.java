/*
 * #%L
 * LambdaCallbackAdapter.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
public final class LambdaCallbackAdapter<V>
        implements StreamCallback<V> {

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
