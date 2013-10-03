/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.message;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.connection.Connection;

/**
 * Provides a convenient mechanism for creating a <a href=
 * "http://www.mongodb.org/display/DOCS/getLastError+Command" >getlasterror</a>
 * command.
 * <p>
 * This is a helper class for retrieving the results of {@link Delete},
 * {@link Insert}, and {@link Update} commands. Get last error is not a part of
 * the standard wire protocol but is provided here due to the frequency of
 * usage.
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GetLastError extends Query {

    /**
     * Creates the query document for the getlasterror command.
     * 
     * @param fsync
     *            If true the command waits for an fsync of the data to have
     *            completed.
     * @param waitForJournal
     *            If true the command waits for the preceding command to have
     *            been written to the journal.
     * @param w
     *            The replication factor to wait for.
     * @param wtimeout
     *            The amount of time (in milliseconds) to wait for the write to
     *            finish.
     * @return The command's query document.
     */
    private static Document createQuery(final boolean fsync,
            final boolean waitForJournal, final int w, final int wtimeout) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);
        if (waitForJournal) {
            builder.addBoolean("j", waitForJournal);
        }
        if (fsync) {
            builder.addBoolean("fsync", fsync);
        }
        if (w >= 1) {
            builder.addInteger("w", w);
        }
        if (wtimeout > 0) {
            builder.addInteger("wtimeout", wtimeout);
        }
        return builder.build();
    }

    /**
     * Create a new GetLastError.
     * 
     * @param dbName
     *            The name of the database.
     * @param fsync
     *            If true the command waits for an fsync of the data to have
     *            completed.
     * @param waitForJournal
     *            If true the command waits for the preceding command to have
     *            been written to the journal.
     * @param w
     *            The replication factor to wait for.
     * @param wtimeout
     *            The amount of time (in milliseconds) to wait for the write to
     *            finish.
     */
    public GetLastError(final String dbName, final boolean fsync,
            final boolean waitForJournal, final int w, final int wtimeout) {
        super(dbName, Connection.COMMAND_COLLECTION, createQuery(fsync,
                waitForJournal, w, wtimeout), /* fields= */null,
        /* batchSize= */1, /* limit= */1, /* numberToSkip= */0,
        /* tailable= */false, ReadPreference.PRIMARY,
        /* noCursorTimeout= */false, /* awaitData= */false,
        /* exhaust= */false, /* partial= */false);
    }

    /**
     * Create a new GetLastError.
     * 
     * @param dbName
     *            The name of the database.
     * @param durability
     *            The Durability requested.
     */
    public GetLastError(final String dbName, final Durability durability) {
        super(dbName, Connection.COMMAND_COLLECTION, durability.asDocument(),
        /* fields= */null, /* batchSize= */1, /* limit= */1,
        /* numberToSkip= */0, /* tailable= */false, ReadPreference.PRIMARY,
        /* noCursorTimeout= */false, /* awaitData= */false,
        /* exhaust= */false, /* partial= */false);
    }
}
