/*
 * #%L
 * LatencyServerSelector.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LatencyServerSelector provides an implementation of the server selector that
 * uses the server latencies to determine the server to select.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LatencyServerSelector
        implements ServerSelector {

    /** The cluster to choose from. */
    private final Cluster myCluster;

    /** If true then only writable servers should be selected. */
    private final boolean myWritableOnly;

    /**
     * Creates a new LatencyServerSelector.
     *
     * @param cluster
     *            The cluster to choose from.
     * @param writableOnly
     *            If true then only writable servers should be selected. If
     *            false then any server (writable and not writable) may be
     *            selected.
     */
    public LatencyServerSelector(final Cluster cluster,
            final boolean writableOnly) {
        myCluster = cluster;
        myWritableOnly = writableOnly;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to order the servers with the lowest latency first.
     * </p>
     */
    @Override
    public List<Server> pickServers() {
        List<Server> servers;
        if (myWritableOnly) {
            servers = myCluster.getWritableServers();
        }
        else {
            servers = myCluster.getServers();
        }

        // If there are no servers then there is no one to pick.
        if (servers.isEmpty()) {
            return Collections.emptyList();
        }

        // Copy to a list we know we can modify and sort.
        servers = new ArrayList<Server>(servers);
        Collections.sort(servers, ServerLatencyComparator.COMPARATOR);

        return servers;
    }
}
