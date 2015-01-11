/*
 * #%L
 * ConnectionFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection;

import java.io.Closeable;
import java.io.IOException;

import com.allanbank.mongodb.client.ClusterStats;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.bootstrap.BootstrapConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.TransportConnection;
import com.allanbank.mongodb.client.metrics.MongoClientMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * Provides an abstraction for constructing a connection. At the lowest level a
 * connection to a MongoDB process is done through a {@link TransportConnection}
 * but there are several connection facades to intelligently connect to Replica
 * Sets and Shard configurations.
 * <p>
 * The {@link BootstrapConnectionFactory} can be used to boot strap the
 * appropriate type of connection factory. It will use a single connection to a
 * MongoDB process to perform a series of commands to determine the server
 * configuration type (Sharded, Replica Set, Standalone) and the setup the
 * appropriate delegate connection factory.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ConnectionFactory
        extends Closeable {

    /**
     * Creates a connection to the address provided.
     *
     * @return The Connection to MongoDB.
     * @throws IOException
     *             On a failure connecting to the server.
     */
    public Connection connect() throws IOException;

    /**
     * Returns the meta-data on the current cluster.
     *
     * @return The meta-data on the current cluster.
     */
    public ClusterStats getClusterStats();

    /**
     * Returns the type of cluster the connection factory connects to.
     *
     * @return The type of cluster the connection factory connects to.
     */
    public ClusterType getClusterType();

    /**
     * Returns the metrics collection agent to the connection factory.
     *
     * @return The metrics agent for the client.
     */
    public MongoClientMetrics getMetrics();

    /**
     * Returns the reconnection strategy for the type of connections.
     *
     * @return The reconnection strategy for the type of connections.
     */
    public ReconnectStrategy getReconnectStrategy();

    /**
     * Adds the metrics collection agent to the connection factory. The
     * connection factory should ensure that all of the connections report
     * metrics via a {@link MongoMessageListener} from the
     * {@link MongoClientMetrics#newConnection(String)} method.
     *
     * @param metrics
     *            The metrics agent for the client.
     */
    public void setMetrics(MongoClientMetrics metrics);
}
