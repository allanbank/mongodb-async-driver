/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.io.Closeable;
import java.io.Flushable;

import com.allanbank.mongodb.MongoDbException;

/**
 * Provides the lowest level interface for interacting with a MongoDB server.
 * The method provided here are a straight forward mapping from the <a href=
 * "http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol">MongoDB Wire
 * Protocol</a>.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Connection extends Closeable, Flushable {

    /** The collection to use when issuing commands to the database. */
    public static final String COMMAND_COLLECTION = "$cmd";

    /**
     * Receives a message from the connection. This method may block until a
     * reply is received or an error occurs.
     * 
     * @return The message received or null if no message is received.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public Message receive() throws MongoDbException;

    /**
     * Sends a message on the connection.
     * 
     * @param messages
     *            The messages to send on the connection. The messages will be
     *            sent one after the other and are guaranteed to be contiguous
     *            and have sequential message ids.
     * @return The request id assigned to the last message. Can be used to
     *         correlate to a read response.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public int send(Message... messages) throws MongoDbException;
}
