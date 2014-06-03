/*
 * #%L
 * AggregateCommand.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.client.VersionRange;

/**
 * Helper class for the aggregation commands.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregateCommand extends Command implements CursorableMessage {

    /** The original aggregation. */
    private final Aggregate myAggregate;

    /**
     * Create a new AggregateCommand.
     * 
     * @param aggregation
     *            The original aggregation.
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection to run the aggregation on.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The preference for which servers to use to retrieve the
     *            results.
     * @param requiredServerVersion
     *            The required version of the server to support processing the
     *            message.
     */
    public AggregateCommand(final Aggregate aggregation,
            final String databaseName, final String collectionName,
            final Document commandDocument,
            final ReadPreference readPreference,
            final VersionRange requiredServerVersion) {
        super(databaseName, collectionName, commandDocument, readPreference,
                requiredServerVersion);

        myAggregate = aggregation;
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     * 
     * @param object
     *            The object to compare to.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            result = super.equals(object);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the batch size from the {@link Aggregate}.
     * </p>
     */
    @Override
    public int getBatchSize() {
        return myAggregate.getBatchSize();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the limit from the {@link Aggregate}.
     * </p>
     */
    @Override
    public int getLimit() {
        return myAggregate.getCursorLimit();
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        return result;
    }
}
