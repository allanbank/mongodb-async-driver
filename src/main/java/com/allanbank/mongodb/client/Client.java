/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Unified client interface to MongoDB.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Client {
    /**
     * The absolute maximum size for a BSON document supported by all versions
     * of MongoDB servers: 16MiB ({@value} ).
     */
    public static final int MAX_DOCUMENT_SIZE = 16 * 1024 * 1024;

    /**
     * Closes the client.
     */
    public void close();

    /**
     * Returns the type of cluster the client is connected to.
     * 
     * @return The type of cluster the client is connected to.
     */
    public ClusterType getClusterType();

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
     * Returns the {@link ReadPreference} from the {@link MongoDbConfiguration}.
     * 
     * @return The default read preference from the {@link MongoDbConfiguration}
     *         .
     */
    public ReadPreference getDefaultReadPreference();

    /**
     * Sends a message on the connection.
     * 
     * @param message
     *            The message to send on the connection.
     * @param replyCallback
     *            The callback to notify of responses to the messages. May be
     *            <code>null</code>.
     * @return The server that was sent the request.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public String send(Message message, Callback<Reply> replyCallback)
            throws MongoDbException;

    /**
     * Sends a message on the connection.
     * 
     * @param message1
     *            The first message to send on the connection.
     * @param message2
     *            The second message to send on the connection.
     * @param replyCallback
     *            The callback to notify of responses to the {@code message2}.
     *            May be <code>null</code>.
     * @return The server that was sent the request.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public String send(Message message1, Message message2,
            Callback<Reply> replyCallback) throws MongoDbException;
}
