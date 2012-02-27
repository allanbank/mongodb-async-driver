/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

/**
 * ServerSelector provides a common interface for a methodology to select a
 * server.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ServerSelector {
    /**
     * Picks a server to try and connect to. The exact criteria used to select a
     * server is defined by the implementation.
     * 
     * @return The selected server to try and connect to.
     */
    public ServerState pickServer();
}
