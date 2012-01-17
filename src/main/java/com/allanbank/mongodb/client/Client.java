/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.messsage.GetLastError;
import com.allanbank.mongodb.connection.messsage.GetMore;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Unified client interface to MongoDB.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Client {
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
     */
    public void send(GetMore getMore, Callback<Reply> callback);

    /**
     * Sends a message.
     * 
     * @param message
     *            The message to send.
     */
    public void send(Message message);

    /**
     * Sends a message followed by a getlasterror message.
     * 
     * @param message
     *            The message to send.
     * @param lastError
     *            The last error command.
     * @param callback
     *            The callback to receive the response to the getlasterror.
     */
    public void send(Message message, GetLastError lastError,
            Callback<Reply> callback);

    /**
     * Sends a query request.
     * 
     * @param query
     *            The query to send.
     * @param callback
     *            The callback to receive the response to the query.
     */
    public void send(Query query, Callback<Reply> callback);
}
