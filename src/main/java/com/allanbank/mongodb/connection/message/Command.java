/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

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
     * @param commandDocument
     *            The command document containing the command and options.
     */
    public Command(final String databaseName, final Document commandDocument) {
        this(databaseName, commandDocument, false);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param replicaOk
     *            If the command can be run on a replica.
     */
    public Command(final String databaseName, final Document commandDocument,
            final boolean replicaOk) {
        super(databaseName, COMMAND_COLLECTION, commandDocument, null, 1, 0,
                false, replicaOk, false, false, false, false);
    }

}
