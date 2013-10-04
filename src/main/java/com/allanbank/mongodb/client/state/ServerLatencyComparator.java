/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.state;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares {@link Server}'s based on the latency of the servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyComparator implements Comparator<Server>,
        Serializable {

    /** A singleton instance of the comparator. No need to multiple instances. */
    public static final Comparator<Server> COMPARATOR = new ServerLatencyComparator();

    /** Serialization version of the class. */
    private static final long serialVersionUID = -7926757327660948536L;

    /**
     * Creates a new {@link ServerLatencyComparator}.
     */
    private ServerLatencyComparator() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Compares the servers based on their respective average latencies.
     * </p>
     * 
     * @see Comparator#compare
     */
    @Override
    public int compare(final Server o1, final Server o2) {
        return Double.compare(o1.getAverageLatency(), o2.getAverageLatency());
    }

}
