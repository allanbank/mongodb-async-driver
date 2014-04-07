/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.io.Closeable;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.client.connection.Connection;

/**
 * A specialization of the {@link ClientImpl} to always try to use the same
 * connection.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SerialClientImpl extends AbstractClient {

    /** If true then assertions have been enabled for the class. */
    protected static final boolean ASSERTIONS_ENABLED;
    static {
        ASSERTIONS_ENABLED = SerialClientImpl.class.desiredAssertionStatus();
    }

    /** The current active Connection to the MongoDB Servers. */
    private Connection myConnection;

    /** The delegate client for accessing connections. */
    private final ClientImpl myDelegate;

    /**
     * Create a new SerialClientImpl.
     *
     * @param client
     *            The delegate client for accessing connections.
     */
    public SerialClientImpl(final ClientImpl client) {
        myDelegate = client;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close all of the open connections.
     * </p>
     *
     * @see Closeable#close()
     */
    @Override
    public void close() {
        super.close();

        // Don't close the delegate.
        myConnection = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the cluster stats from the delegate client.
     * </p>
     */
    @Override
    public ClusterStats getClusterStats() {
        return myDelegate.getClusterStats();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the {@link ClusterType} of delegate
     * {@link ClientImpl}.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return myDelegate.getClusterType();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configuration used when the client was
     * constructed.
     * </p>
     */
    @Override
    public MongoClientConfiguration getConfig() {
        return myDelegate.getConfig();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configurations default durability.
     * </p>
     *
     * @see Client#getDefaultDurability()
     */
    @Override
    public Durability getDefaultDurability() {
        return myDelegate.getDefaultDurability();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configurations default read preference.
     * </p>
     *
     * @see Client#getDefaultReadPreference()
     */
    @Override
    public ReadPreference getDefaultReadPreference() {
        return myDelegate.getDefaultReadPreference();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the delegate client.
     * </p>
     *
     * @see Client#restart(DocumentAssignable)
     */
    @Override
    public MongoIterator<Document> restart(
            final DocumentAssignable cursorDocument)
            throws IllegalArgumentException {
        return myDelegate.restart(cursorDocument);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the delegate client.
     * </p>
     *
     * @see Client#restart(StreamCallback, DocumentAssignable)
     */
    @Override
    public MongoCursorControl restart(final StreamCallback<Document> results,
            final DocumentAssignable cursorDocument)
            throws IllegalArgumentException {
        return myDelegate.restart(results, cursorDocument);
    }

    /**
     * Tries to reuse the last connection used. If the connection it closed or
     * does not exist then the request is delegated to the {@link ClientImpl}
     * and the result cached for future requests.
     *
     * @return The found connection.
     * @throws MongoDbException
     *             On a failure to talk to the MongoDB servers.
     */
    @Override
    protected Connection findConnection(final Message message1,
            final Message message2) throws MongoDbException {
        if ((myConnection == null) || !myConnection.isAvailable()) {
            myConnection = myDelegate.findConnection(message1, message2);
        }

        return myConnection;
    }
}
