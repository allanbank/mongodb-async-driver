/*
 * #%L
 * Client.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.client.callback.ReplyCallback;

/**
 * Unified client interface to MongoDB.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
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
     * Returns the meta-data on the current cluster.
     *
     * @return The meta-data on the current cluster.
     */
    public ClusterStats getClusterStats();

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
    public MongoClientConfiguration getConfig();

    /**
     * Returns the {@link Durability} from the {@link MongoClientConfiguration}.
     *
     * @return The default durability from the {@link MongoClientConfiguration}.
     */
    public Durability getDefaultDurability();

    /**
     * Returns the {@link ReadPreference} from the
     * {@link MongoClientConfiguration}.
     *
     * @return The default read preference from the
     *         {@link MongoClientConfiguration} .
     */
    public ReadPreference getDefaultReadPreference();

    /**
     * Restarts an iterator that was previously saved.
     *
     * @param cursorDocument
     *            The document containing the state of the cursor.
     * @return The restarted iterator.
     * @throws IllegalArgumentException
     *             If the document does not contain a valid cursor state.
     */
    public MongoIterator<Document> restart(DocumentAssignable cursorDocument)
            throws IllegalArgumentException;

    /**
     * Restarts a document stream from a cursor that was previously saved.
     *
     * @param results
     *            Callback that will be notified of the results of the cursor.
     * @param cursorDocument
     *            The document containing the state of the cursor.
     * @return A {@link MongoCursorControl} to control the cursor streaming
     *         documents to the caller. This includes the ability to stop the
     *         cursor and persist its state.
     * @throws IllegalArgumentException
     *             If the document does not contain a valid cursor state.
     */
    public MongoCursorControl restart(final StreamCallback<Document> results,
            DocumentAssignable cursorDocument) throws IllegalArgumentException;

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
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public void send(Message message1, Message message2,
            ReplyCallback replyCallback) throws MongoDbException;

    /**
     * Sends a message on the connection.
     *
     * @param message
     *            The message to send on the connection.
     * @param replyCallback
     *            The callback to notify of responses to the messages. May be
     *            <code>null</code>.
     * @throws MongoDbException
     *             On an error sending the message.
     */
    public void send(Message message, ReplyCallback replyCallback)
            throws MongoDbException;
}
