/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.io.IOException;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;

/**
 * Common interface for all MongoDB messages read from and sent to a MongoDB
 * server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Message {

    /**
     * Returns the name of the database.
     * 
     * @return The name of the database.
     */
    public String getDatabaseName();

    /**
     * Provides the details on which servers are eligible to receive the
     * message.
     * 
     * @return The {@link ReadPreference} for which servers should be sent the
     *         request.
     */
    public ReadPreference getReadPreference();

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
