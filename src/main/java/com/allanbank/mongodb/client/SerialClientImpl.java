/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.io.Closeable;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * A specialization of the {@link ClientImpl} to always try to use the same
 * connection.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SerialClientImpl implements Client {

    /** The logger for the {@link SerialClientImpl}. */
    protected static final Logger LOG = Logger.getLogger(SerialClientImpl.class
            .getCanonicalName());

    /** The delegate client for accessing connections. */
    private final ClientImpl myDelegate;

    /** The current active Connection to the MongoDB Servers. */
    private Connection myConnection;

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
        // Don't close the delegate.
        myConnection = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the configuration used when the client was
     * constructed.
     * </p>
     */
    @Override
    public MongoDbConfiguration getConfig() {
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
     * Overridden to locate the .
     * </p>
     * 
     * @see Client#send(GetMore,Callback)
     */
    @Override
    public void send(final GetMore getMore, final Callback<Reply> callback) {
        findConnection().send(callback, getMore);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} to MongoDB.
     * </p>
     * 
     * @see Client#send(Message)
     */
    @Override
    public void send(final Message message) {
        findConnection().send(message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Message} and {@link GetLastError} to
     * MongoDB.
     * </p>
     * 
     * @see Client#send(Message, GetLastError, Callback)
     */
    @Override
    public void send(final Message message, final GetLastError lastError,
            final Callback<Reply> callback) {
        findConnection().send(callback, message, lastError);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link Query} to MongoDB.
     * </p>
     * 
     * @see Client#send(Query, Callback)
     */
    @Override
    public void send(final Query query, final Callback<Reply> callback) {
        findConnection().send(callback, query);
    }

    /**
     * Tries to locate a connection that can quickly dispatch the message to a
     * MongoDB server. The basic metrics for determining if a connection is idle
     * is to look at the number of messages waiting to be sent. The basic logic
     * for finding a connection is:
     * <ol>
     * <li>Scan the list of connection looking for an idle connection. If one is
     * found use it.</li>
     * <li>If there are no idle connections determine the maximum number of
     * allowed connections and if there are fewer that the maximum allowed then
     * take the connection creation lock, create a new connection, use it, and
     * add to the set of available connections and release the lock.</li>
     * <li>If there are is still not a connection idle then sort the connections
     * based on a snapshot of pending messages and use the connection with the
     * least messages.</li>
     * <ul>
     * 
     * @return The found connection.
     * @throws MongoDbException
     *             On a failure to talk to the MongoDB servers.
     */
    protected Connection findConnection() throws MongoDbException {
        if (myConnection == null || !myConnection.isOpen()) {
            myConnection = myDelegate.findConnection();
        }
        return myConnection;
    }
}
