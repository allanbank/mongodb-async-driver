/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection;

import java.io.IOException;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * Common interface for all MongoDB messages read from and sent to a MongoDB
 * server.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * Validates that the documents with the message do not exceed the maximum
     * document size specified.
     * 
     * @param visitor
     *            The {@link SizeOfVisitor} to compute the size of the document.
     * @param maxDocumentSize
     *            The maximum document size to validate against.
     * @throws DocumentToLargeException
     *             If one of the documents in the message is too large or the
     *             documents in aggregate are too large.
     */
    public void validateSize(SizeOfVisitor visitor, int maxDocumentSize)
            throws DocumentToLargeException;

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
