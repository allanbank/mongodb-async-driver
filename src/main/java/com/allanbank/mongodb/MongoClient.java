/*
 * #%L
 * MongoClient.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import java.io.Closeable;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;

/**
 * Interface to bootstrap into interactions with MongoDB.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoClient extends Closeable {

    /**
     * Returns a MongoClient instance that shares connections with this
     * MongoClient instance but serializes all of its requests on a single
     * connection.
     * <p>
     * While the returned MongoClient instance is thread safe it is intended to
     * be used by a single logical thread to ensure requests issued to the
     * MongoDB server are guaranteed to be processed in the same order they are
     * requested.
     * </p>
     * <p>
     * Creation of the serial instance is lightweight with minimal object
     * allocation and no server interaction.
     * </p>
     * 
     * @return A serialized view of the MongoDB connections.
     */
    public MongoClient asSerializedClient();

    /**
     * Returns the configuration being used by the logical MongoDB connection.
     * 
     * @return The configuration being used by the logical MongoDB connection.
     */
    public MongoClientConfiguration getConfig();

    /**
     * Returns the MongoDatabase with the specified name. This method does not
     * validate that the database already exists in the MongoDB instance.
     * 
     * @param name
     *            The name of the existing database.
     * @return The {@link MongoDatabase}.
     */
    public MongoDatabase getDatabase(String name);

    /**
     * Returns a list of database names.
     * 
     * @return A list of available database names.
     */
    public List<String> listDatabaseNames();

    /**
     * Returns a list of database names.
     * 
     * @return A list of available database names.
     * @deprecated Use the {@link #listDatabaseNames()} method instead.
     */
    @Deprecated
    public List<String> listDatabases();

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
     * <p>
     * The sequence of callbacks will be terminated by either calling the
     * {@link LambdaCallback#accept(Throwable, Object) results.accept(...)}
     * method with <code>null</code> for both parameters or by calling the
     * method with an error for the first parameter.
     * </p>
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
    public MongoCursorControl restart(final LambdaCallback<Document> results,
            DocumentAssignable cursorDocument) throws IllegalArgumentException;

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

}
