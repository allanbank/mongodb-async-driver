/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import org.junit.Test;

/**
 * ServerLatencyComparatorTest provides testss for the
 * {@link ServerLatencyComparator}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyComparatorTest {

    /**
     * Test method for
     * {@link ServerLatencyComparator#compare(ServerState, ServerState)}.
     */
    @Test
    public void testCompareLess() {
        final ServerState state1 = new ServerState(new InetSocketAddress(1024));
        final ServerState state2 = new ServerState(new InetSocketAddress(1025));

        state1.setAverageLatency(99.0);
        state2.setAverageLatency(100.0);

        assertEquals(-1,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

    /**
     * Test method for
     * {@link ServerLatencyComparator#compare(ServerState, ServerState)}.
     */
    @Test
    public void testCompareMore() {
        final ServerState state1 = new ServerState(new InetSocketAddress(1024));
        final ServerState state2 = new ServerState(new InetSocketAddress(1025));

        state1.setAverageLatency(101.0);
        state2.setAverageLatency(100.0);

        assertEquals(1,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

    /**
     * Test method for
     * {@link ServerLatencyComparator#compare(ServerState, ServerState)}.
     */
    @Test
    public void testCompareSame() {
        final ServerState state1 = new ServerState(new InetSocketAddress(1024));
        final ServerState state2 = new ServerState(new InetSocketAddress(1025));

        state1.setAverageLatency(100.0);
        state2.setAverageLatency(100.0);

        assertEquals(0,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

}
