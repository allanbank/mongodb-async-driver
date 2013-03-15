/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Logger;

import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.util.IOUtils;

/**
 * SimpleReconnectStrategy provides a reconnection strategy to simply attempt to
 * connect to the server again.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategy extends AbstractReconnectStrategy {

    /** The logger for the {@link SimpleReconnectStrategy}. */
    protected static final Logger LOG = Logger
            .getLogger(SimpleReconnectStrategy.class.getCanonicalName());

    /**
     * Creates a new SimpleReconnectStrategy.
     */
    public SimpleReconnectStrategy() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to try and connect to every server selected by the
     * ServerSelector until one of the connections is successful.
     * </p>
     */
    @Override
    public Connection reconnect(final Connection oldConnection) {

        final List<ServerState> servers = getSelector().pickServers();

        // Clear the interrupt state for the thread.
        final boolean wasInterrupted = Thread.interrupted();
        try {
            for (final ServerState server : servers) {
                Connection newConn = null;
                try {
                    final InetSocketAddress addr = server.getServer();

                    newConn = getConnectionFactory().connect(server,
                            getConfig());
                    if (isConnected(server, newConn)) {

                        LOG.info("Reconnected to " + addr);

                        copyPending(newConn, oldConnection);

                        return newConn;
                    }

                    IOUtils.close(newConn);
                }
                catch (final IOException error) {
                    // Connection failed.
                    // Try the next one.
                    LOG.fine("Reconnect to " + server + " failed: "
                            + error.getMessage());
                }
            }
        }
        finally {
            // Reset the interrupt state.
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("Reconnect attempt failed for all " + servers.size()
                + " servers: " + servers);

        return null;
    }
}
