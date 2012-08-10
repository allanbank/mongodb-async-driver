/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * ClusterStateTest provides tests for the {@link ClusterState}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterStateTest {

    /**
     * Test method for {@link ClusterState#add(String)}.
     */
    @Test
    public void testAdd() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        state.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, state.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#cdf(List)}.
     */
    @Test
    public void testCdf() {

        final List<ServerState> servers = new ArrayList<ServerState>(5);
        ServerState server = new ServerState("localhost:1024");
        server.setAverageLatency(100);
        servers.add(server);

        server = new ServerState("localhost:1025");
        server.setAverageLatency(100);
        servers.add(server);

        server = new ServerState("localhost:1026");
        server.setAverageLatency(200);
        servers.add(server);

        server = new ServerState("localhost:1027");
        server.setAverageLatency(200);
        servers.add(server);

        server = new ServerState("localhost:1028");
        server.setAverageLatency(1000);
        servers.add(server);

        final double relativeSum = 1 + 1 + 2 + 2 + 10;

        final ClusterState state = new ClusterState();
        Collections.shuffle(servers);
        final double[] cdf = state.cdf(servers);

        assertEquals((1D / relativeSum), cdf[0], 0.00001);
        assertEquals(((1D / relativeSum) + (1D / relativeSum)), cdf[1], 0.00001);
        assertEquals(
                ((1D / relativeSum) + (1D / relativeSum) + (2D / relativeSum)),
                cdf[2], 0.00001);
        assertEquals(((1D / relativeSum) + (1D / relativeSum)
                + (2D / relativeSum) + (2D / relativeSum)), cdf[3], 0.00001);
        assertEquals(
                ((1D / relativeSum) + (1D / relativeSum) + (2D / relativeSum)
                        + (2D / relativeSum) + (10D / relativeSum)), cdf[4],
                0.00001);
        assertEquals(1.0, cdf[4], 0.000001); // CDF should always end at 1.0
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersNearest() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        s1.setAverageLatency(1);
        s2.setAverageLatency(10);
        s3.setAverageLatency(100);

        c.markNotWritable(s1);
        c.markWritable(s2);

        List<ServerState> servers = c.findCandidateServers(ReadPreference
                .closest());
        assertEquals(3, servers.size());
        final double last = Double.NEGATIVE_INFINITY;
        double lowest = Double.MAX_VALUE;
        for (final ServerState server : servers.subList(1, servers.size())) {
            lowest = Math.min(lowest, server.getAverageLatency());
            assertTrue(
                    "Latencies out of order: " + last + " !< "
                            + server.getAverageLatency(),
                    last < server.getAverageLatency());
        }

        // Exclude on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s2.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.closest(BuilderFactory
                .start().addInteger("f", 1).build(), BuilderFactory.start()
                .addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s2));
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimary() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        assertEquals(Collections.emptyList(),
                c.findCandidateServers(ReadPreference.PRIMARY));

        c.markWritable(s2);
        assertEquals(Collections.singletonList(s2),
                c.findCandidateServers(ReadPreference.PRIMARY));

        c.markWritable(s3);
        assertEquals(
                new HashSet<ServerState>(Arrays.asList(s2, s3)),
                new HashSet<ServerState>(c
                        .findCandidateServers(ReadPreference.PRIMARY)));

        c.markNotWritable(s2);
        assertEquals(Collections.singletonList(s3),
                c.findCandidateServers(ReadPreference.PRIMARY));

        c.markWritable(s1);
        c.markNotWritable(s3);
        assertEquals(Collections.singletonList(s1),
                c.findCandidateServers(ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimaryPreferred() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        s1.setAverageLatency(1);
        s2.setAverageLatency(10);
        s3.setAverageLatency(100);

        c.markNotWritable(s1);
        c.markNotWritable(s2);
        c.markWritable(s3);

        List<ServerState> servers = c.findCandidateServers(ReadPreference
                .preferPrimary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Writable first.
        assertEquals(new HashSet<ServerState>(Arrays.asList(s1, s2)),
                new HashSet<ServerState>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        c.markNotWritable(s1);
        c.markNotWritable(s2);
        c.markNotWritable(s3);
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("C", 1).build()));
        assertEquals(1, servers.size());
        assertEquals(s1, servers.get(0));

        // Exclude all on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondary() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        c.markWritable(s1);
        c.markWritable(s2);
        c.markWritable(s3);

        assertEquals(Collections.emptyList(),
                c.findCandidateServers(ReadPreference.secondary()));

        c.markNotWritable(s2);
        assertEquals(Collections.singletonList(s2),
                c.findCandidateServers(ReadPreference.secondary()));

        c.markNotWritable(s3);
        assertEquals(
                new HashSet<ServerState>(Arrays.asList(s2, s3)),
                new HashSet<ServerState>(c.findCandidateServers(ReadPreference
                        .secondary())));

        c.markWritable(s2);
        assertEquals(Collections.singletonList(s3),
                c.findCandidateServers(ReadPreference.secondary()));

        c.markNotWritable(s1);
        c.markWritable(s3);
        assertEquals(Collections.singletonList(s1),
                c.findCandidateServers(ReadPreference.secondary()));

        // Exclude on tags.
        c.markNotWritable(s1);
        c.markNotWritable(s2);
        c.markNotWritable(s3);

        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("A", 1).build());
        final List<ServerState> servers = c.findCandidateServers(ReadPreference
                .secondary(BuilderFactory.start().addInteger("f", 1).build(),
                        BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(1, servers.size());
        assertEquals(s1, servers.get(0));

    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondaryPreferred() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        s1.setAverageLatency(1);
        s2.setAverageLatency(10);
        s3.setAverageLatency(100);

        c.markWritable(s1);
        c.markWritable(s2);
        c.markNotWritable(s3);

        List<ServerState> servers = c.findCandidateServers(ReadPreference
                .preferSecondary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Non-Writable first.
        assertEquals(new HashSet<ServerState>(Arrays.asList(s1, s2)),
                new HashSet<ServerState>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.preferSecondary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        // Exclude all on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = c.findCandidateServers(ReadPreference.preferSecondary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersServer() {
        final ClusterState c = new ClusterState();
        final ServerState s1 = c.add("localhost:27017");
        final ServerState s2 = c.add("localhost:27018");
        final ServerState s3 = c.add("localhost:27019");

        assertEquals(
                Collections.singletonList(s1),
                c.findCandidateServers(ReadPreference.server("localhost:27017")));
        assertEquals(
                Collections.singletonList(s2),
                c.findCandidateServers(ReadPreference.server("localhost:27018")));
        assertEquals(
                Collections.singletonList(s3),
                c.findCandidateServers(ReadPreference.server("localhost:27019")));
        assertSame(
                Collections.emptyList(),
                c.findCandidateServers(ReadPreference.server("localhost:27020")));
        assertSame(
                Collections.emptyList(),
                c.findCandidateServers(ReadPreference.server("localhost:27020")));
    }

    /**
     * Test method for {@link ClusterState#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        state.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, state.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#getNonWritableServers()}.
     */
    @Test
    public void testGetNonWritableServers() {
        final ClusterState state = new ClusterState();

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markWritable(ss);
        assertTrue(ss.isWritable());

        assertEquals(Collections.singletonList(ss), state.getServers());
        assertEquals(Collections.singletonList(ss), state.getWritableServers());
        assertEquals(0, state.getNonWritableServers().size());

        state.markNotWritable(ss);
        assertFalse(ss.isWritable());

        assertEquals(Collections.singletonList(ss), state.getServers());
        assertEquals(Collections.singletonList(ss),
                state.getNonWritableServers());
        assertEquals(0, state.getWritableServers().size());

    }

    /**
     * Test method for {@link ClusterState#markNotWritable(ServerState)}.
     */
    @Test
    public void testMarkNotWritable() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markWritable(ss);

        assertTrue(ss.isWritable());

        state.addListener(mockListener);

        state.markNotWritable(ss);
        assertFalse(ss.isWritable());

        state.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.TRUE, event.getValue().getOldValue());
        assertEquals(Boolean.FALSE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#markWritable(ServerState)}.
     */
    @Test
    public void testMarkWritable() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markNotWritable(ss);

        assertFalse(ss.isWritable());

        state.addListener(mockListener);

        state.markWritable(ss);
        assertTrue(ss.isWritable());
        state.markWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for
     * {@link ClusterState#removeListener(PropertyChangeListener)}.
     */
    @Test
    public void testRemoveListener() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markNotWritable(ss);

        assertFalse(ss.isWritable());

        state.addListener(mockListener);

        state.markWritable(ss);
        assertTrue(ss.isWritable());
        state.markWritable(ss);

        state.removeListener(mockListener);

        state.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#sort(List)}.
     */
    @Test
    public void testSortListServerState() {

        final int count = 1000;
        final List<ServerState> servers = new ArrayList<ServerState>(count);
        for (int i = 0; i < count; i++) {

            final ServerState server = new ServerState("localhost:"
                    + (i + 1024));
            server.setAverageLatency(Math.random() * 100000);

            servers.add(server);
        }

        final ClusterState state = new ClusterState();
        for (int i = 0; i < 100; ++i) {
            Collections.shuffle(servers);

            state.sort(servers);

            // Verify that the list is sorted EXCEPT the first server.
            final double last = Double.NEGATIVE_INFINITY;
            double lowest = Double.MAX_VALUE;
            for (final ServerState server : servers.subList(1, count)) {
                lowest = Math.min(lowest, server.getAverageLatency());
                assertTrue(
                        "Latencies out of order: " + last + " !< "
                                + server.getAverageLatency(),
                        last < server.getAverageLatency());
            }
        }
    }
}
