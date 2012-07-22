/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Unified client interface to MongoDB.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Client {
    /**
     * Closes the client.
     */
    public void close();

    /**
     * Returns the configuration being used by the logical MongoDB connection.
     * 
     * @return The configuration being used by the logical MongoDB connection.
     */
    public MongoDbConfiguration getConfig();

    /**
     * Returns the {@link Durability} from the {@link MongoDbConfiguration}.
     * 
     * @return The default durability from the {@link MongoDbConfiguration}.
     */
    public Durability getDefaultDurability();

    /**
     * Sends a get more request.
     * 
     * @param getMore
     *            The get more message to send.
     * @param callback
     *            The callback to receive the response to the get more.
     * @throws MongoDbException
     *             On a failure to communicate with the MongoDB servers.
     */
    public void send(GetMore getMore, Callback<Reply> callback)
            throws MongoDbException;

    /**
     * Sends a message.
     * 
     * @param message
     *            The message to send.
     * @throws MongoDbException
     *             On a failure to communicate with the MongoDB servers.
     */
    public void send(Message message) throws MongoDbException;

    /**
     * Sends a message followed by a getlasterror message.
     * 
     * @param message
     *            The message to send.
     * @param lastError
     *            The last error command.
     * @param callback
     *            The callback to receive the response to the getlasterror.
     * @throws MongoDbException
     *             On a failure to communicate with the MongoDB servers.
     */
    public void send(Message message, GetLastError lastError,
            Callback<Reply> callback) throws MongoDbException;

    /**
     * Sends a query request.
     * 
     * @param query
     *            The query to send.
     * @param callback
     *            The callback to receive the response to the query.
     * @throws MongoDbException
     *             On a failure to communicate with the MongoDB servers.
     */
    public void send(Query query, Callback<Reply> callback)
            throws MongoDbException;
}
