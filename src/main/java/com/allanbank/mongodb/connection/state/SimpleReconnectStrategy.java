/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;

/**
 * SimpleReconnectStrategy provides a reconnection strategy to simply attempt to
 * connect to the server again.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategy extends
        AbstractReconnectStrategy<Connection> {

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

                newConn = getConnectionFactory().connect(addr, getConfig());
                if (isConnected(server, newConn)) {

                    LOG.info("Reconnected to " + addr);

                    copyPending(newConn, oldConnection);

                    return newConn;
                }

                IOUtils.close(newConn);
            }
            catch (final MongoDbException error) {
                IOUtils.close(newConn);
                newConn = null;
            }
            catch (final IOException error) {
                IOUtils.close(newConn);
            }
        }

        return null;
    }
}
