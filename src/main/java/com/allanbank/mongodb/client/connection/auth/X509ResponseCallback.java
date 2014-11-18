/*
 * #%L
 * X509ResponseCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.callback.AbstractValidatingReplyCallback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * X509ResponseCallback provides the callback for all of the X509 requests.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class X509ResponseCallback extends
        AbstractValidatingReplyCallback {

    /** The future to update with the results. */
    private final FutureCallback<Boolean> myResults;

    /**
     * Creates a new X509ResponseCallback.
     * 
     * @param results
     *            The future to update with the results.
     */
    public X509ResponseCallback(final FutureCallback<Boolean> results) {
        myResults = results;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to update the results with the failure.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myResults.exception(thrown);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle each SASL response until the exchange is complete.
     * </p>
     */
    @Override
    public void handle(final Reply reply) {
        final MongoDbException exception = asError(reply);
        if (exception != null) {
            exception(exception);
            return;
        }

        // Otherwise if we got an OK response then we are authenticated.
        myResults.callback(Boolean.TRUE);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }
}