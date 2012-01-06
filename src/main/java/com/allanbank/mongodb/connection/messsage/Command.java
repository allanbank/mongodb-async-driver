/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.messsage;

import com.allanbank.mongodb.bson.Document;

/**
 * Helper class to make generating command queries easier.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Command extends Query {

    /** The collection to use when issuing commands to the database. */
    public static final String COMMAND_COLLECTION = "$cmd";

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection.
     * @param commandDocument
     *            The command document containing the command and options.
     */
    public Command(final String databaseName, final Document commandDocument) {
        super(databaseName, COMMAND_COLLECTION, commandDocument, null, 1, 0,
                false, false, false, false, false, false);
    }

}
