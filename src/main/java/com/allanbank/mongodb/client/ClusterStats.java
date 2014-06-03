/*
 * #%L
 * ClusterStats.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
