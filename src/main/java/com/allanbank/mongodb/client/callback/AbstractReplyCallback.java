/*
 * #%L
 * AbstractReplyCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * Helper class for constructing callbacks that convert a {@link Reply} message
 * into a different type of result.
 *
 * @param <F>
 *            The type for the converted {@link Reply}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractReplyCallback<F>
        extends AbstractValidatingReplyCallback {

    /** The callback for the converted type. */
    final Callback<F> myForwardCallback;

    /**
     * Create a new AbstractReplyCallback.
     *
     * @param forwardCallback
     *            The callback for the converted type.
     */
    public AbstractReplyCallback(final Callback<F> forwardCallback) {
        myForwardCallback = forwardCallback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward the exception to the {@link #myForwardCallback}.
     * </p>
     *
     * @see Callback#exception
     */
    @Override
    public void exception(final Throwable thrown) {
        getForwardCallback().exception(thrown);
    }

    /**
     * Returns the forwardCallback value.
     *
     * @return the forwardCallback
     */
    public Callback<F> getForwardCallback() {
        return myForwardCallback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true if the {@link #getForwardCallback()
     * forwardCallback} is a {@link FutureCallback} or to forward the call if
     * the {@code forwardCallback} is a {@link ReplyCallback}.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return (myForwardCallback instanceof FutureCallback)
                || ((myForwardCallback instanceof ReplyCallback) && ((ReplyCallback) myForwardCallback)
                        .isLightWeight());
    }

    /**
     * Converts the {@link Reply} into the final response type.
     *
     * @param reply
     *            The reply to convert.
     * @return The converted reply.
     * @throws MongoDbException
     *             On a failure converting the reply. Generally, the
     *             {@link #verify(Reply)} method should be used to report
     *             errors.
     */
    protected abstract F convert(Reply reply) throws MongoDbException;

    /**
     * {@inheritDoc}
     * <p>
     * Overriden to {@link #convert(Reply) convert} and then pass the converted
     * reply to the {@link #getForwardCallback() forward callback}.
     * </p>
     */
    @Override
    protected void handle(final Reply reply) {
        getForwardCallback().callback(convert(reply));

    }
}
