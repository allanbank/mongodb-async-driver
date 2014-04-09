/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.proxy;

import com.allanbank.mongodb.client.connection.Connection;

/**
 * ConnectionInfo provides a container for the information on a discovered
 * server.
 * 
 * @param <K>
 *            The type of key used to identify the server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConnectionInfo<K> {

    /** The connection to the primary server. */
    private final Connection myConnection;

    /** The primary server. */
    private final K myConnectionKey;

    /**
     * Creates a new ConnectionInfo.
     * 
     * @param connection
     *            The connection to the primary server.
     * @param connectionKey
     *            The primary server.
     */
    public ConnectionInfo(final Connection connection, final K connectionKey) {
        super();
        this.myConnection = connection;
        this.myConnectionKey = connectionKey;
    }

    /**
     * Returns the connection to the primary server.
     * 
     * @return The connection to the primary server.
     */
    public Connection getConnection() {
        return myConnection;
    }

    /**
     * Returns the primary server.
     * 
     * @return The primary server.
     */
    public K getConnectionKey() {
        return myConnectionKey;
    }
}
