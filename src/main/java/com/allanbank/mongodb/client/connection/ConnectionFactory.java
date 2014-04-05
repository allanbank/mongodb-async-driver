/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection;

import java.io.Closeable;
import java.io.IOException;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.bootstrap.BootstrapConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.SocketConnection;

/**
 * Provides an abstraction for constructing a connection. At the lowest level a
 * connection to a MongoDB process is done through a {@link SocketConnection}
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
public interface ConnectionFactory extends Closeable {

    /**
     * Creates a connection to the address provided.
     *
     * @return The Connection to MongoDB.
     * @throws IOException
     *             On a failure connecting to the server.
     */
    public Connection connect() throws IOException;

    /**
     * Returns the type of cluster the connection factory connects to.
     *
     * @return The type of cluster the connection factory connects to.
     */
    public ClusterType getClusterType();

    /**
     * Returns the maximum server version within the cluster.
     *
     * @return The maximum server version within the cluster.
     */
    public Version getMaximumServerVersion();

    /**
     * Returns the minimum server version within the cluster.
     *
     * @return The minimum server version within the cluster.
     */
    public Version getMinimumServerVersion();

    /**
     * Returns the reconnection strategy for the type of connections.
     *
     * @return The reconnection strategy for the type of connections.
     */
    public ReconnectStrategy getReconnectStrategy();

    /**
     * Returns smallest value for the maximum number of write operations allowed
     * in a single write command.
     *
     * @return The smallest value for maximum number of write operations allowed
     *         in a single write command.
     */
    public int getSmallestMaxBatchedWriteOperations();

    /**
     * Returns the smallest value for the maximum BSON object size within the
     * cluster.
     *
     * @return The smallest value for the maximum BSON object size within the
     *         cluster.
     */
    public long getSmallestMaxBsonObjectSize();
}
