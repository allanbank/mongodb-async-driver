/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;

/**
 * Helper class to make generating command queries easier.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
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
        this(databaseName, commandDocument, ReadPreference.PRIMARY);
    }

    /**
     * Create a new Command.
     * 
     * @param databaseName
     *            The name of the database.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     */
    public Command(final String databaseName, final Document commandDocument,
            final ReadPreference readPreference) {
        super(databaseName, COMMAND_COLLECTION, commandDocument,
        /* fields= */null,
        /* batchSize= */1, /* limit= */1, /* numberToSkip= */0,
        /* tailable= */false, readPreference,
        /* noCursorTimeout= */false, /* awaitData= */false,
        /* exhaust= */false, /* partial= */false);
    }

}
