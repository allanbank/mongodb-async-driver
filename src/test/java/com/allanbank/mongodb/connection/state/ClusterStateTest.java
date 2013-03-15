/*
 * Copyright 2012-2013, Allanbank Consulting, InmyState. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.easymock.EasyMock.createMock;
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.Connection;

/**
 * ClusterStateTest provides tests for the {@link ClusterState}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, InmyState., All Rights Reserved
 */
public class ClusterStateTest {
    /** The pinger being tested. */
    protected ClusterState myState = null;

    /**
     * Cleans up the pinger.
     */
    @After
    public void tearDown() {
        myState = null;
    }

    /**
     * Test method for {@link ClusterState#add(String)}.
     */
    @Test
    public void testAdd() {
        myState = new ClusterState(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        myState.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, myState.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#cdf(List)}.
     */
    @Test
    public void testCdf() {

        final List<ServerState> servers = new ArrayList<ServerState>(5);
        ServerState server = new ServerState(new InetSocketAddress(1024));
        server.updateAverageLatency(100);
        servers.add(server);

        server = new ServerState(new InetSocketAddress(1025));
        server.updateAverageLatency(100);
        servers.add(server);

        server = new ServerState(new InetSocketAddress(1026));
        server.updateAverageLatency(200);
        servers.add(server);

        server = new ServerState(new InetSocketAddress(1027));
        server.updateAverageLatency(200);
        servers.add(server);

        server = new ServerState(new InetSocketAddress(1028));
        server.updateAverageLatency(1000);
        servers.add(server);

        final double relativeSum = 1 + 1 + (1D / 2) + (1D / 2) + (1D / 10);

        myState = new ClusterState(new MongoClientConfiguration());
        Collections.shuffle(servers);
        final double[] cdf = myState.cdf(servers);

        assertEquals((1D / relativeSum), cdf[0], 0.00001);
        assertEquals(((1D / relativeSum) + (1D / relativeSum)), cdf[1], 0.00001);
        assertEquals(
                ((1D / relativeSum) + (1D / relativeSum) + ((1D / 2D) / relativeSum)),
                cdf[2], 0.00001);
        assertEquals(((1D / relativeSum) + (1D / relativeSum)
                + ((1D / 2D) / relativeSum) + ((1D / 2D) / relativeSum)),
                cdf[3], 0.00001);
        assertEquals(
                ((1D / relativeSum) + (1D / relativeSum)
                        + ((1D / 2D) / relativeSum) + ((1D / 2D) / relativeSum) + ((1D / 10D) / relativeSum)),
                cdf[4], 0.00001);
        assertEquals(1.0, cdf[4], 0.000001); // CDF should always end at 1.0
    }

    /**
     * Test method for {@link ClusterState#close}.
     * 
     * @throws IOException
     *             On a failure setting up the mock connection.
     */
    @Test
    public void testClose() throws IOException {

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        myState = new ClusterState(new MongoClientConfiguration());
        myState.add("localhost:27017").addConnection(mockConnection);
        myState.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersNearest() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        myState.markNotWritable(s1);
        myState.markWritable(s2);

        List<ServerState> servers = myState.findCandidateServers(ReadPreference
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
        servers = myState.findCandidateServers(ReadPreference.closest(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s2));
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimary() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        assertEquals(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        myState.markWritable(s2);
        assertEquals(Collections.singletonList(s2),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        myState.markWritable(s3);
        assertEquals(
                new HashSet<ServerState>(Arrays.asList(s2, s3)),
                new HashSet<ServerState>(myState
                        .findCandidateServers(ReadPreference.PRIMARY)));

        myState.markNotWritable(s2);
        assertEquals(Collections.singletonList(s3),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        myState.markWritable(s1);
        myState.markNotWritable(s3);
        assertEquals(Collections.singletonList(s1),
                myState.findCandidateServers(ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimaryPreferred() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        myState.markNotWritable(s1);
        myState.markNotWritable(s2);
        myState.markWritable(s3);

        List<ServerState> servers = myState.findCandidateServers(ReadPreference
                .preferPrimary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Writable first.
        assertEquals(new HashSet<ServerState>(Arrays.asList(s1, s2)),
                new HashSet<ServerState>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        myState.markNotWritable(s1);
        myState.markNotWritable(s2);
        myState.markNotWritable(s3);
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s3));

        myState.markWritable(s1);
        myState.markWritable(s2);
        myState.markWritable(s3);
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s3));

        // Exclude all on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondary() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        myState.markWritable(s1);
        myState.markWritable(s2);
        myState.markWritable(s3);

        assertEquals(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference.secondary()));

        myState.markNotWritable(s2);
        assertEquals(Collections.singletonList(s2),
                myState.findCandidateServers(ReadPreference.secondary()));

        myState.markNotWritable(s3);
        assertEquals(
                new HashSet<ServerState>(Arrays.asList(s2, s3)),
                new HashSet<ServerState>(myState
                        .findCandidateServers(ReadPreference.secondary())));

        myState.markWritable(s2);
        assertEquals(Collections.singletonList(s3),
                myState.findCandidateServers(ReadPreference.secondary()));

        myState.markNotWritable(s1);
        myState.markWritable(s3);
        assertEquals(Collections.singletonList(s1),
                myState.findCandidateServers(ReadPreference.secondary()));

        // Exclude on tags.
        myState.markNotWritable(s1);
        myState.markNotWritable(s2);
        myState.markNotWritable(s3);

        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("A", 1).build());
        final List<ServerState> servers = myState
                .findCandidateServers(ReadPreference.secondary(BuilderFactory
                        .start().addInteger("f", 1).build(), BuilderFactory
                        .start().addInteger("g", 1).build()));
        assertEquals(1, servers.size());
        assertEquals(s1, servers.get(0));

    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondaryPreferred() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        myState.markWritable(s1);
        myState.markWritable(s2);
        myState.markNotWritable(s3);

        List<ServerState> servers = myState.findCandidateServers(ReadPreference
                .preferSecondary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Non-Writable first.
        assertEquals(new HashSet<ServerState>(Arrays.asList(s1, s2)),
                new HashSet<ServerState>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferSecondary(
                BuilderFactory.start().addInteger("f", 1).build(),
                BuilderFactory.start().addInteger("g", 1).build()));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        // Exclude all on tags.
        s1.setTags(BuilderFactory.start().addInteger("f", 1).build());
        s3.setTags(BuilderFactory.start().addInteger("g", 1).build());
        servers = myState.findCandidateServers(ReadPreference.preferSecondary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondaryPreferredWithVeryLateServer() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        // Should not be used. Too old.
        s1.setSecondsBehind(TimeUnit.HOURS.toSeconds(1));

        myState.markNotWritable(s1);
        myState.markWritable(s2);
        myState.markNotWritable(s3);

        final List<ServerState> servers = myState
                .findCandidateServers(ReadPreference.preferSecondary());
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0)); // Non-Writable first.
        assertEquals(new HashSet<ServerState>(Arrays.asList(s2)),
                new HashSet<ServerState>(servers.subList(1, servers.size())));
    }

    /**
     * Test method for {@link ClusterState#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersServer() {
        myState = new ClusterState(new MongoClientConfiguration());
        final ServerState s1 = myState.add("localhost:27017");
        final ServerState s2 = myState.add("localhost:27018");
        final ServerState s3 = myState.add("localhost:27019");

        assertEquals(Collections.singletonList(s1),
                myState.findCandidateServers(ReadPreference
                        .server("localhost:27017")));
        assertEquals(Collections.singletonList(s2),
                myState.findCandidateServers(ReadPreference
                        .server("localhost:27018")));
        assertEquals(Collections.singletonList(s3),
                myState.findCandidateServers(ReadPreference
                        .server("localhost:27019")));
        assertSame(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference
                        .server("localhost:27020")));
        assertSame(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference
                        .server("localhost:27020")));
    }

    /**
     * Test method for {@link ClusterState#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        myState = new ClusterState(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        myState.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, myState.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#getNonWritableServers()}.
     */
    @Test
    public void testGetNonWritableServers() {
        myState = new ClusterState(new MongoClientConfiguration());

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        myState.markWritable(ss);
        assertTrue(ss.isWritable());

        assertEquals(Collections.singletonList(ss), myState.getServers());
        assertEquals(Collections.singletonList(ss),
                myState.getWritableServers());
        assertEquals(0, myState.getNonWritableServers().size());

        myState.markNotWritable(ss);
        assertFalse(ss.isWritable());

        assertEquals(Collections.singletonList(ss), myState.getServers());
        assertEquals(Collections.singletonList(ss),
                myState.getNonWritableServers());
        assertEquals(0, myState.getWritableServers().size());

    }

    /**
     * Test method for {@link ClusterState#markNotWritable(ServerState)}.
     */
    @Test
    public void testMarkNotWritable() {
        myState = new ClusterState(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        myState.markWritable(ss);

        assertTrue(ss.isWritable());

        myState.addListener(mockListener);

        myState.markNotWritable(ss);
        assertFalse(ss.isWritable());

        myState.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertEquals(Boolean.TRUE, event.getValue().getOldValue());
        assertEquals(Boolean.FALSE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#markWritable(ServerState)}.
     */
    @Test
    public void testMarkWritable() {
        myState = new ClusterState(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        myState.markNotWritable(ss);

        assertFalse(ss.isWritable());

        myState.addListener(mockListener);

        myState.markWritable(ss);
        assertTrue(ss.isWritable());
        myState.markWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for
     * {@link ClusterState#removeListener(PropertyChangeListener)}.
     */
    @Test
    public void testRemoveListener() {
        myState = new ClusterState(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = myState.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        myState.markNotWritable(ss);

        assertFalse(ss.isWritable());

        myState.addListener(mockListener);

        myState.markWritable(ss);
        assertTrue(ss.isWritable());
        myState.markWritable(ss);

        myState.removeListener(mockListener);

        myState.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
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

            final ServerState server = new ServerState(new InetSocketAddress(
                    "localhost:", i + 1024));
            server.updateAverageLatency(Math.random() * 100000);

            servers.add(server);
        }

        myState = new ClusterState(new MongoClientConfiguration());
        for (int i = 0; i < 100; ++i) {
            Collections.shuffle(servers);

            myState.sort(servers);

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
