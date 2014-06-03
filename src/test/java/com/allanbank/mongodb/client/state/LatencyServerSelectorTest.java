/*
 * #%L
 * LatencyServerSelectorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.ClusterType;

/**
 * LatencyServerSelectorTest provides tests for the
 * {@link LatencyServerSelector} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LatencyServerSelectorTest {

    /**
     * Test method for {@link LatencyServerSelector#pickServers()}.
     */
    @Test
    public void testPickServersAll() {

        final Document primary = BuilderFactory.start().add("ismaster", true)
                .build();
        final Document secondary = BuilderFactory.start()
                .add("ismaster", false).add("secondary", true).build();

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);

        cluster.add("localhost:27017").updateAverageLatency(100);
        cluster.add("localhost:27018").updateAverageLatency(50);
        cluster.add("localhost:27019").updateAverageLatency(25);
        cluster.add("localhost:27020").updateAverageLatency(200);
        cluster.add("localhost:27021").updateAverageLatency(300);
        cluster.add("localhost:27022").updateAverageLatency(400);

        cluster.get("localhost:27017").update(primary);
        cluster.get("localhost:27018").update(secondary);
        cluster.get("localhost:27019").update(primary);
        cluster.get("localhost:27020").update(secondary);
        cluster.get("localhost:27021").update(primary);
        cluster.get("localhost:27022").update(secondary);

        final List<Server> expected = new ArrayList<Server>();
        expected.add(cluster.get("localhost:27019")); // 25.
        expected.add(cluster.get("localhost:27018")); // 50.
        expected.add(cluster.get("localhost:27017")); // 100.
        expected.add(cluster.get("localhost:27020")); // 200.
        expected.add(cluster.get("localhost:27021")); // 300.
        expected.add(cluster.get("localhost:27022")); // 400.

        final LatencyServerSelector selector = new LatencyServerSelector(
                cluster, false);
        assertEquals(expected, selector.pickServers());
    }

    /**
     * Test method for {@link LatencyServerSelector#pickServers()}.
     */
    @Test
    public void testPickServersNone() {

        final Document secondary = BuilderFactory.start()
                .add("ismaster", false).add("secondary", true).build();

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);

        cluster.add("localhost:27017").updateAverageLatency(100);
        cluster.add("localhost:27018").updateAverageLatency(50);
        cluster.add("localhost:27019").updateAverageLatency(25);
        cluster.add("localhost:27020").updateAverageLatency(200);
        cluster.add("localhost:27021").updateAverageLatency(300);
        cluster.add("localhost:27022").updateAverageLatency(400);

        cluster.get("localhost:27017").update(secondary);
        cluster.get("localhost:27018").update(secondary);
        cluster.get("localhost:27019").update(secondary);
        cluster.get("localhost:27020").update(secondary);
        cluster.get("localhost:27021").update(secondary);
        cluster.get("localhost:27022").update(secondary);

        final LatencyServerSelector selector = new LatencyServerSelector(
                cluster, true);
        assertSame(Collections.emptyList(), selector.pickServers());
    }

    /**
     * Test method for {@link LatencyServerSelector#pickServers()}.
     */
    @Test
    public void testPickServersWriteableOnly() {

        final Document primary = BuilderFactory.start().add("ismaster", true)
                .build();
        final Document secondary = BuilderFactory.start()
                .add("ismaster", false).add("secondary", true).build();

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);

        cluster.add("localhost:27017").updateAverageLatency(100);
        cluster.add("localhost:27018").updateAverageLatency(50);
        cluster.add("localhost:27019").updateAverageLatency(25);
        cluster.add("localhost:27020").updateAverageLatency(200);
        cluster.add("localhost:27021").updateAverageLatency(300);
        cluster.add("localhost:27022").updateAverageLatency(400);

        cluster.get("localhost:27017").update(primary);
        cluster.get("localhost:27018").update(secondary);
        cluster.get("localhost:27019").update(primary);
        cluster.get("localhost:27020").update(secondary);
        cluster.get("localhost:27021").update(primary);
        cluster.get("localhost:27022").update(secondary);

        final List<Server> expected = new ArrayList<Server>();
        expected.add(cluster.get("localhost:27019")); // 25.
        expected.add(cluster.get("localhost:27017")); // 100.
        expected.add(cluster.get("localhost:27021")); // 300.

        final LatencyServerSelector selector = new LatencyServerSelector(
                cluster, true);
        assertEquals(expected, selector.pickServers());
    }
}
