/*
 * #%L
 * ServerLatencyComparatorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
