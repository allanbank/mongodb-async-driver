/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.Cluster;
import com.allanbank.mongodb.connection.state.ServerSelector;

/**
 * ReconnectStrategy provides a common interface for a strategy for reconnecting
 * to a MongoDB server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ReconnectStrategy {

    /**
     * Sets the configuration to be used by the reconnection strategy.
     * 
     * @param config
     *            The configuration for the connections.
     */
    public void setConfig(MongoClientConfiguration config);

    /**
     * Sets the connection factory to use to establish connections to the
     * server.
     * 
     * @param connectionFactory
     *            The connection factory to use to establish connections to the
     *            server.
     */
    public void setConnectionFactory(ProxiedConnectionFactory connectionFactory);

    /**
     * Sets the selector to be used by the reconnection strategy.
     * 
     * @param selector
     *            The selector for connections.
     */
    public void setSelector(ServerSelector selector);

    /**
     * Sets the state of the cluster to be used by the reconnection strategy.
     * 
     * @param state
     *            The state of the cluster.
     */
    public void setState(Cluster state);

    /**
     * Encapsulates the strategy for re-establishing the connection to the
     * server.
     * 
     * @param oldConnection
     *            The connection that has become disconnected.
     * @return The new connection to the server or null if a connection could
     *         not be created.
     */
    Connection reconnect(Connection oldConnection);
}
