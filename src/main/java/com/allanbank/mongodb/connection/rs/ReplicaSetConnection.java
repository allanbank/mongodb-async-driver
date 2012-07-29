/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;

/**
 * Provides a {@link Connection} implementation for connecting to a replica-set
 * environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnection extends AbstractProxyConnection {

    /** The logger for the {@link ReplicaSetConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ReplicaSetConnection.class.getCanonicalName());

    /** A connection to a Secondary Replica. */
    private Connection mySecondaryConnection;

    /**
     * Creates a new {@link ReplicaSetConnection}.
     * 
     * @param proxiedConnection
     *            The connection being proxied.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnection(final Connection proxiedConnection,
            final MongoDbConfiguration config) {
        super(proxiedConnection, config);
        mySecondaryConnection = null;
    }

    /**
     * Closes the underlying connection.
     * 
     * @see Connection#close()
     */
    @Override
    public void close() throws IOException {
        try {
            if (mySecondaryConnection != null) {
                mySecondaryConnection.close();
                mySecondaryConnection = null;
            }
        }
        finally {
            super.close();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Checks if all if the messages are queries and can use a secondary
     * connection. If so then if a secondary connection is present it is used to
     * send the messages. Otherwise (or in the case of an error from the
     * secondary connection) the primary connection is used.
     * </p>
     */
    @Override
    public void send(final Callback<Reply> reply, final Message... messages)
            throws MongoDbException {

        boolean canUseSecondary = true;
        for (final Message message : messages) {
            if ((message instanceof Query)
                    && (((Query) message).getReadPreference() != null)) {
                canUseSecondary &= ((Query) message).getReadPreference()
                        .isSecondaryOk();
            }
            else {
                canUseSecondary = false;
            }
        }

        final Connection secondary = mySecondaryConnection;
        if (canUseSecondary && (secondary != null)) {
            try {
                secondary.send(reply, messages);
            }
            catch (final MongoDbException error) {
                // Failed. Try the primary.
                mySecondaryConnection = null;
                try {
                    secondary.close();
                }
                catch (final IOException ignore) {
                    LOG.log(Level.INFO,
                            "Failure sending a request to the secondary, using primary.",
                            ignore);
                }
            }
        }

        super.send(reply, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link #send(Callback, Message...)} method with
     * a <code>null</code> callback.
     * </p>
     */
    @Override
    public void send(final Message... messages) throws MongoDbException {
        send(null, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "ReplicaSet(" + getProxiedConnection() + ")";
    }

    /**
     * Returns the current secondary connection.
     * 
     * @return The current secondary connection (which may be <code>null</code>
     *         ).
     */
    protected Connection getSecondaryConnection() {
        return mySecondaryConnection;
    }

    /**
     * Sets the secondary connection to be used.
     * 
     * @param connection
     *            The secondary connection to be used.
     */
    protected void setSecondaryConnection(final Connection connection) {
        mySecondaryConnection = connection;
    }
}
