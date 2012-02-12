/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.Connection;

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
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
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
        if (w > 1) {
            builder.addInteger("w", w);
        }
        if (wtimeout > 0) {
            builder.addInteger("wtimeout", wtimeout);
        }
        return builder.get();
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
                waitForJournal, w, wtimeout), null, 1, 0, false, false, false,
                false, false, false);
    }
}
