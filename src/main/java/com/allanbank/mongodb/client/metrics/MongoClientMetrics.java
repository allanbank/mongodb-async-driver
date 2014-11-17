/*
 * #%L
 * MongoClientMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.Closeable;

/**
 * MongoClientMetrics provides the interface for exposing various client
 * metrics.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoClientMetrics extends Closeable {

    /**
     * Notification that client has been closed and any resources for the client
     * can be cleaned up.
     */
    @Override
    public void close();

    /**
     * Notification that a connection has been closed and any resources for the
     * connection can be cleaned up.
     * 
     * @param listener
     *            The listener for the messages from the connection to the
     *            server. Any further interaction with the MongoMessageListener
     *            is undefined.
     */
    public void connectionClosed(MongoMessageListener listener);

    /**
     * Returns a listener for the messages from a single connection to the
     * server.
     * 
     * @param serverName
     *            The name of the server for the connection.
     * @return A listener for the messages from a single connection to the
     *         server.
     */
    public ConnectionMetricsCollector newConnection(String serverName);

    /**
     * Adds a listener to be notified of all messages sent and received. Setting
     * this listener can have a severe impact on the performance of the driver
     * and should be avoided.
     * 
     * @param listener
     *            The listener for all messages sent or received by the cluster.
     */
    public void setMessageListener(MongoMessageListener listener);
}
