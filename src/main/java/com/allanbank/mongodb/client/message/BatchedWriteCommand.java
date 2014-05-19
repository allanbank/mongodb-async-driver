/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.client.VersionRange;

/**
 * BatchedWriteCommand provides a container to hold the batched write command
 * and the operations that it was created from.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteCommand extends Command {

    /** The required server version range for the {@link BatchedWrite} command. */
    public static final VersionRange REQUIRED_VERSION_RANGE = VersionRange
            .minimum(BatchedWrite.REQUIRED_VERSION);

    /** The bundle for the batched write. */
    private final BatchedWrite.Bundle myBundle;

    /**
     * Create a new BatchedWriteCommand.
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not
     *            {@link Command#COMMAND_COLLECTION $cmd} if the real collection
     *            is known.
     * @param bundle
     *            The bundle for the batched write.
     */
    public BatchedWriteCommand(final String databaseName,
            final String collectionName, final BatchedWrite.Bundle bundle) {
        super(databaseName, collectionName, bundle.getCommand(),
                ReadPreference.PRIMARY, REQUIRED_VERSION_RANGE);

        myBundle = bundle;
    }

    /**
     * Creates a new BatchedWriteCommand. This constructor is provided for tests
     * that do not want to create a
     * {@link com.allanbank.mongodb.builder.BatchedWrite.Bundle}
     * 
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not
     *            {@link Command#COMMAND_COLLECTION $cmd} if the real collection
     *            is known.
     * @param commandDocument
     *            The batch command document.
     */
    public BatchedWriteCommand(final String databaseName,
            final String collectionName, final Document commandDocument) {
        super(databaseName, collectionName, commandDocument,
                ReadPreference.PRIMARY, REQUIRED_VERSION_RANGE);
        myBundle = null;
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
     * Returns the bundle for the batched write.
     * 
     * @return The bundle for the batched write.
     */
    public BatchedWrite.Bundle getBundle() {
        return myBundle;
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
