/*
 * #%L
 * LongToIntCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.callback;

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
