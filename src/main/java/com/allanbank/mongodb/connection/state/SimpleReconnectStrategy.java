/*
 * Copyright 2012, Allanbank Consulting, Inc. 
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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

        for (final ServerState server : servers) {
            Connection newConn = null;
            try {
                final InetSocketAddress addr = server.getServer();

                newConn = getConnectionFactory().connect(server, getConfig());
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
                error.hashCode(); // PMD - Shhhh.
            }
        }

        return null;
    }
}
