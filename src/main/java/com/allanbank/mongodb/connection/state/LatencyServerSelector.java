/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LatencyServerSelector provides an implementation of the server selector that
 * uses the server latencies to determine the server to select.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LatencyServerSelector implements ServerSelector {

    /** The cluster to choose from. */
    private final ClusterState myCluster;

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
    public LatencyServerSelector(final ClusterState cluster,
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
    public List<ServerState> pickServers() {
        List<ServerState> servers;
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
        servers = new ArrayList<ServerState>(servers);
        Collections.sort(servers, ServerLatencyComparator.COMPARATOR);

        return servers;
    }
}
