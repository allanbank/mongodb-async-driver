/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.io.IOException;

import com.allanbank.mongodb.bson.io.BsonOutputStream;

/**
 * Common interface for all MongoDB messages read from and sent to a MongoDB
 * server.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Message {

    /**
     * Writes the message from the stream. The message header <b>is</b> written
     * by this method.
     * 
     * @param messageId
     *            The id to be assigned to the message.
     * @param out
     *            The sink for data written.
     * @throws IOException
     *             On an error writing to the stream.
     */
    public void write(int messageId, BsonOutputStream out) throws IOException;
}
