/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.state.Server;

/**
 * ReplicaSetConnectionInfo provides a container for the information on a
 * discovered primary server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class ReplicaSetConnectionInfo {

    /** The connection to the primary server. */
    private final Connection myConnection;

    /** The primary server. */
    private final Server myPrimaryServer;

    /**
     * Creates a new ReplicaSetConnectionInfo.
     * 
     * @param connection
     *            The connection to the primary server.
     * @param primaryServer
     *            The primary server.
     */
    public ReplicaSetConnectionInfo(final Connection connection,
            final Server primaryServer) {
        super();
        this.myConnection = connection;
        this.myPrimaryServer = primaryServer;
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
    public Server getPrimaryServer() {
        return myPrimaryServer;
    }
}
