/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.state;

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
     * Test method for {@link ServerLatencyComparator#compare(Server, Server)}.
     */
    @Test
    public void testCompareLess() {
        final Server state1 = new Server(new InetSocketAddress(1024));
        final Server state2 = new Server(new InetSocketAddress(1025));

        state1.updateAverageLatency(99);
        state2.updateAverageLatency(100);

        assertEquals(-1,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

    /**
     * Test method for {@link ServerLatencyComparator#compare(Server, Server)}.
     */
    @Test
    public void testCompareMore() {
        final Server state1 = new Server(new InetSocketAddress(1024));
        final Server state2 = new Server(new InetSocketAddress(1025));

        state1.updateAverageLatency(101);
        state2.updateAverageLatency(100);

        assertEquals(1,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

    /**
     * Test method for {@link ServerLatencyComparator#compare(Server, Server)}.
     */
    @Test
    public void testCompareSame() {
        final Server state1 = new Server(new InetSocketAddress(1024));
        final Server state2 = new Server(new InetSocketAddress(1025));

        state1.updateAverageLatency(100);
        state2.updateAverageLatency(100);

        assertEquals(0,
                ServerLatencyComparator.COMPARATOR.compare(state1, state2));
    }

}
