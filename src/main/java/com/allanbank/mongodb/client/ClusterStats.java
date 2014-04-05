/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

/**
 * ClusterInformation provides information on the cluster.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ClusterStats {
    /**
     * Returns the range of versions that we currently see within the cluster.
     *
     * @return Tthe range of versions that we currently see within the cluster.
     */
    public VersionRange getServerVersionRange();

    /**
     * Returns smallest value for the maximum number of write operations allowed
     * in a single write command.
     *
     * @return The smallest value for maximum number of write operations allowed
     *         in a single write command.
     */
    public int getSmallestMaxBatchedWriteOperations();

    /**
     * Returns the lowest value for the maximum BSON object size within the
     * cluster.
     *
     * @return The lowest value for the maximum BSON object size within the
     *         cluster.
     */
    public long getSmallestMaxBsonObjectSize();
}
