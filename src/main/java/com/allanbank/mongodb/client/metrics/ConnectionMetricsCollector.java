/*
 * #%L
 * MongoMessageListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics;

/**
 * ConnectionMetricsCollector provides the interface for connections to publish
 * that a message has been sent/received and for users to be notified that
 * messages have been sent and received.
 * <p>
 * <em>Note</em>: This interface receives all of the messages at the core of the
 * driver and is extremely performance sensitive.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ConnectionMetricsCollector
        extends MongoMessageListener {

    /**
     * Notification that the connection has been closed.
     */
    public void close();
}
