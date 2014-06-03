/*
 * #%L
 * LegacyStreamCallbackAdapter.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;

/**
 * LegacyStreamCallbackAdapter provides an adapter from a {@link Callback} to a
 * {@link StreamCallback}. This adapter simulates the old behaviour specified in
 * the
 * {@link MongoCollection#streamingFind(Callback, com.allanbank.mongodb.bson.DocumentAssignable)}
 * and
 * {@link MongoCollection#streamingFind(Callback, com.allanbank.mongodb.builder.Find)}
 * using the new interface.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @deprecated Deprecated to ensure this class is removed with the
 *             {@link MongoCollection#streamingFind(Callback, com.allanbank.mongodb.bson.DocumentAssignable)}
 *             and
 *             {@link MongoCollection#streamingFind(Callback, com.allanbank.mongodb.builder.Find)}
 *             methods.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public final class LegacyStreamCallbackAdapter implements
        StreamCallback<Document> {

    /** The legacy callback to delegate to. */
    private final Callback<Document> myDelegate;

    /**
     * Creates a new LegacyStreamCallbackAdapter.
     * 
     * @param delegate
     *            The legacy callback to delegate to.
     */
    public LegacyStreamCallbackAdapter(final Callback<Document> delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the legacy {@link Callback#callback}.
     * </p>
     */
    @Override
    public void callback(final Document result) {
        myDelegate.callback(result);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to {@link Callback#callback callback(null)}.
     * </p>
     */
    @Override
    public void done() {
        myDelegate.callback(null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the legacy {@link Callback#exception}.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myDelegate.exception(thrown);
    }
}