/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import java.io.IOException;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * Provides an interface for creating proxied connections.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ProxiedConnectionFactory extends ConnectionFactory {
    /**
     * Creates a connection to the address provided.
     * 
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @return The Connection to MongoDB.
     * @throws IOException
     *             On a failure connecting to the server.
     */
    public Connection connect(ServerState server, MongoDbConfiguration config)
            throws IOException;

}
