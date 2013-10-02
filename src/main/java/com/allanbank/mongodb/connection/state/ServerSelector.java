/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.List;

/**
 * ServerSelector provides a common interface for a methodology to select a
 * server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ServerSelector {
    /**
     * Picks a server to try and connect to. The exact criteria used to select a
     * server is defined by the implementation.
     * 
     * @return The selected server to try and connect to.
     */
    public List<Server> pickServers();
}
