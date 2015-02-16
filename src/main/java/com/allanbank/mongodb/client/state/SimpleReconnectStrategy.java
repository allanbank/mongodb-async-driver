/*
 * #%L
 * SimpleReconnectStrategy.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.client.state;

import java.io.IOException;
import java.util.List;

import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * SimpleReconnectStrategy provides a reconnection strategy to simply attempt to
 * connect to the server again.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategy
        extends AbstractReconnectStrategy {

    /** The logger for the {@link SimpleReconnectStrategy}. */
    protected static final Log LOG = LogFactory
            .getLog(SimpleReconnectStrategy.class);

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

        // Clear the interrupt state for the thread.
        final boolean wasInterrupted = Thread.interrupted();
        try {
            // First try and connect back to the original server. This will
            // hopefully re-enable the state.
            final Server origServer = oldConnection.getServer();
            Connection newConn = tryConnect(origServer);
            if (newConn != null) {
                return newConn;
            }

            final List<Server> servers = getSelector().pickServers();
            for (final Server server : servers) {
                newConn = tryConnect(server);
                if (newConn != null) {
                    return newConn;
                }
            }

            LOG.info("Reconnect attempt failed for all {} servers: {}",
                    servers.size(), servers);
        }
        finally {
            // Reset the interrupt state.
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        return null;
    }

    /**
     * Tries to connect to the server.
     *
     * @param server
     *            The server to connect to.
     * @return The connection to the server.
     */
    private Connection tryConnect(final Server server) {
        Connection newConn = null;
        try {

            newConn = getConnectionFactory().connect(server, getConfig());
            if (isConnected(server, newConn)) {
                LOG.info("Reconnected to {}", server);

                // Make sure we don't close the connection.
                final Connection result = newConn;
                newConn = null;

                return result;
            }
        }
        catch (final IOException error) {
            // Connection failed. Try the next one.
            LOG.debug("Reconnect to {} failed: {}", server, error.getMessage());
        }
        finally {
            IOUtils.close(newConn);
        }
        return null;
    }
}
