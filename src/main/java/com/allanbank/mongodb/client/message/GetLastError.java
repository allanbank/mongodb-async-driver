/*
 * #%L
 * GetLastError.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.message;

import java.io.StringWriter;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
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

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to returns the getLastError on a single line.
     * </p>
     */
    @Override
    public String toString() {
        final StringWriter sink = new StringWriter();

        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                sink, true);
        getQuery().accept(visitor);

        return sink.toString();
    }
}
