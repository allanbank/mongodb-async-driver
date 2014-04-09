/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.ParallelScan;
import com.allanbank.mongodb.client.VersionRange;

/**
 * Helper class for the {@code parallelCollectionScan} commands.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ParallelScanCommand extends Command implements CursorableMessage {

    /**
     * The first version of MongoDB to support the
     * {@code parallelCollectionScan} command.
     */
    public static final Version REQUIRED_VERSION = ParallelScan.REQUIRED_VERSION;

    /** The original request. */
    private final ParallelScan myCommand;

    /** The name of the collection to run the {@code parallelCollectionScan} on. */
    private final String myScannedCollectionName;

    /**
     * Create a new ParallelScanCommand.
     * 
     * @param command
     *            The original request.
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection to run the scan on.
     * @param commandDocument
     *            The command document containing the command and options.
     * @param readPreference
     *            The read preference for the command.
     */
    public ParallelScanCommand(final ParallelScan command,
            final String databaseName, final String collectionName,
            final Document commandDocument, final ReadPreference readPreference) {
        super(databaseName, commandDocument, readPreference, VersionRange
                .minimum(REQUIRED_VERSION));

        myCommand = command;
        myScannedCollectionName = collectionName;
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
            final ParallelScanCommand other = (ParallelScanCommand) object;

            result = myScannedCollectionName
                    .equals(other.myScannedCollectionName)
                    && super.equals(object);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return 0 or the default batch size.
     * </p>
     */
    @Override
    public int getBatchSize() {
        return myCommand.getBatchSize();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the name to run the {@code parallelCollectionScan}
     * on instead of the {@link Command#COMMAND_COLLECTION} name.
     * </p>
     */
    @Override
    public String getCollectionName() {
        return myScannedCollectionName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return 0 or no limit.
     * </p>
     */
    @Override
    public int getLimit() {
        return 0;
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
        result = (31 * result) + myScannedCollectionName.hashCode();
        return result;
    }
}
