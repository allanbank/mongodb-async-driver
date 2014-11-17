/*
 * #%L
 * NoOpMongoClientMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics.noop;

import com.allanbank.mongodb.client.metrics.MongoClientMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.NoOpMongoMessageListener;

/**
 * NoOpMongoClientMetrics provides a {@link MongoClientMetrics} that does not
 * collect any metrics.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpMongoClientMetrics implements MongoClientMetrics {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void close() {
        // NoOp.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void connectionClosed(final MongoMessageListener listener) {
        // NoOp.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the {@link NoOpMongoMessageListener}.
     * </p>
     */
    @Override
    public NoOpMongoMessageListener newConnection(final String serverName) {
        return NoOpMongoMessageListener.NO_OP;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void setMessageListener(final MongoMessageListener listener) {
        // NoOp.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to indicate that metrics are disabled.
     * </p>
     */
    @Override
    public String toString() {
        return "Metrics not being collected.";
    }

}
