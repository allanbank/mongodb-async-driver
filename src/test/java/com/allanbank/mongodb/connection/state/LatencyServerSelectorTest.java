/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;

/**
 * LatencyServerSelectorTest provides tests for the
 * {@link LatencyServerSelector} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LatencyServerSelectorTest {

    /**
     * Test method for {@link LatencyServerSelector#pickServers()}.
     */
    @Test
    public void testPickServersAll() {

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());

        cluster.add("localhost:27017").setAverageLatency(100);
        cluster.add("localhost:27018").setAverageLatency(50);
        cluster.add("localhost:27019").setAverageLatency(25);
        cluster.add("localhost:27020").setAverageLatency(200);
        cluster.add("localhost:27021").setAverageLatency(300);
        cluster.add("localhost:27022").setAverageLatency(400);

        cluster.markWritable(cluster.get("localhost:27017"));
        cluster.markNotWritable(cluster.get("localhost:27018"));
        cluster.markWritable(cluster.get("localhost:27019"));
        cluster.markNotWritable(cluster.get("localhost:27020"));
        cluster.markWritable(cluster.get("localhost:27021"));
        cluster.markNotWritable(cluster.get("localhost:27022"));

        final List<ServerState> expected = new ArrayList<ServerState>();
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

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());

        cluster.add("localhost:27017").setAverageLatency(100);
        cluster.add("localhost:27018").setAverageLatency(50);
        cluster.add("localhost:27019").setAverageLatency(25);
        cluster.add("localhost:27020").setAverageLatency(200);
        cluster.add("localhost:27021").setAverageLatency(300);
        cluster.add("localhost:27022").setAverageLatency(400);

        cluster.markNotWritable(cluster.get("localhost:27017"));
        cluster.markNotWritable(cluster.get("localhost:27018"));
        cluster.markNotWritable(cluster.get("localhost:27019"));
        cluster.markNotWritable(cluster.get("localhost:27020"));
        cluster.markNotWritable(cluster.get("localhost:27021"));
        cluster.markNotWritable(cluster.get("localhost:27022"));

        final LatencyServerSelector selector = new LatencyServerSelector(
                cluster, true);
        assertSame(Collections.emptyList(), selector.pickServers());
    }

    /**
     * Test method for {@link LatencyServerSelector#pickServers()}.
     */
    @Test
    public void testPickServersWriteableOnly() {

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());

        cluster.add("localhost:27017").setAverageLatency(100);
        cluster.add("localhost:27018").setAverageLatency(50);
        cluster.add("localhost:27019").setAverageLatency(25);
        cluster.add("localhost:27020").setAverageLatency(200);
        cluster.add("localhost:27021").setAverageLatency(300);
        cluster.add("localhost:27022").setAverageLatency(400);

        cluster.markWritable(cluster.get("localhost:27017"));
        cluster.markNotWritable(cluster.get("localhost:27018"));
        cluster.markWritable(cluster.get("localhost:27019"));
        cluster.markNotWritable(cluster.get("localhost:27020"));
        cluster.markWritable(cluster.get("localhost:27021"));
        cluster.markNotWritable(cluster.get("localhost:27022"));

        final List<ServerState> expected = new ArrayList<ServerState>();
        expected.add(cluster.get("localhost:27019")); // 25.
        expected.add(cluster.get("localhost:27017")); // 100.
        expected.add(cluster.get("localhost:27021")); // 300.

        final LatencyServerSelector selector = new LatencyServerSelector(
                cluster, true);
        assertEquals(expected, selector.pickServers());
    }
}
