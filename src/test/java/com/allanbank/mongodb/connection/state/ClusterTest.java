/*
 * Copyright 2012-2013, Allanbank Consulting, InmyState. 
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;

/**
 * ClusterTest provides tests for the {@link Cluster}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterTest {

    /** The tags for an F server. */
    private static final Document F_TAGS = new ImmutableDocument(BuilderFactory
            .start().addInteger("f", 1));

    /** The tags for an G server. */
    private static final Document G_TAGS = new ImmutableDocument(BuilderFactory
            .start().addInteger("g", 1));

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

    /** Update document to mark servers as the secondary. */
    private static final Document SECONDARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", false)
                    .add("secondary", true));

    /** The builder for miscellaneous documents. */
    private final DocumentBuilder myBuilder = BuilderFactory.start();

    /** The cluster being tested. */
    private Cluster myState = null;

    /**
     * Cleans up the cluster.
     */
    @After
    public void tearDown() {
        myState = null;
        myBuilder.reset();
    }

    /**
     * Test method for {@link Cluster#add(String)}.
     */
    @Test
    public void testAdd() {
        myState = new Cluster(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        myState.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final Server ss = myState.add("foo");
        assertEquals("foo:" + Server.DEFAULT_PORT, ss.getCanonicalName());

        assertSame(ss, myState.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link Cluster#cdf(List)}.
     */
    @Test
    public void testCdf() {

        final List<Server> servers = new ArrayList<Server>(5);
        Server server = new Server(new InetSocketAddress(1024));
        server.updateAverageLatency(100);
        servers.add(server);

        server = new Server(new InetSocketAddress(1025));
        server.updateAverageLatency(100);
        servers.add(server);

        server = new Server(new InetSocketAddress(1026));
        server.updateAverageLatency(200);
        servers.add(server);

        server = new Server(new InetSocketAddress(1027));
        server.updateAverageLatency(200);
        servers.add(server);

        server = new Server(new InetSocketAddress(1028));
        server.updateAverageLatency(1000);
        servers.add(server);

        final double relativeSum = 1 + 1 + (1D / 2) + (1D / 2) + (1D / 10);

        myState = new Cluster(new MongoClientConfiguration());
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
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersNearest() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        s1.update(SECONDARY_UPDATE);
        s2.update(PRIMARY_UPDATE);

        List<Server> servers = myState.findCandidateServers(ReadPreference
                .closest());
        assertEquals(3, servers.size());
        final double last = Double.NEGATIVE_INFINITY;
        double lowest = Double.MAX_VALUE;
        for (final Server server : servers.subList(1, servers.size())) {
            lowest = Math.min(lowest, server.getAverageLatency());
            assertTrue(
                    "Latencies out of order: " + last + " !< "
                            + server.getAverageLatency(),
                    last < server.getAverageLatency());
        }

        // Exclude on tags.
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s2.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.closest(F_TAGS,
                G_TAGS));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s2));
    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimary() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        assertEquals(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        s2.update(PRIMARY_UPDATE);
        assertEquals(Collections.singletonList(s2),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        s3.update(PRIMARY_UPDATE);
        assertEquals(
                new HashSet<Server>(Arrays.asList(s2, s3)),
                new HashSet<Server>(myState
                        .findCandidateServers(ReadPreference.PRIMARY)));

        s2.update(SECONDARY_UPDATE);
        assertEquals(Collections.singletonList(s3),
                myState.findCandidateServers(ReadPreference.PRIMARY));

        s1.update(PRIMARY_UPDATE);
        s3.update(SECONDARY_UPDATE);
        assertEquals(Collections.singletonList(s1),
                myState.findCandidateServers(ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersPrimaryPreferred() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        s1.update(SECONDARY_UPDATE);
        s2.update(SECONDARY_UPDATE);
        s3.update(PRIMARY_UPDATE);

        List<Server> servers = myState.findCandidateServers(ReadPreference
                .preferPrimary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Writable first.
        assertEquals(new HashSet<Server>(Arrays.asList(s1, s2)),
                new HashSet<Server>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                F_TAGS, G_TAGS));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        s1.update(SECONDARY_UPDATE);
        s2.update(SECONDARY_UPDATE);
        s3.update(SECONDARY_UPDATE);
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                F_TAGS, G_TAGS));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s3));

        s1.update(PRIMARY_UPDATE);
        s2.update(PRIMARY_UPDATE);
        s3.update(PRIMARY_UPDATE);
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                F_TAGS, G_TAGS));
        assertEquals(2, servers.size());
        assertTrue(servers.contains(s1));
        assertTrue(servers.contains(s3));

        // Exclude all on tags.
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferPrimary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondary() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        s1.update(PRIMARY_UPDATE);
        s2.update(PRIMARY_UPDATE);
        s3.update(PRIMARY_UPDATE);

        assertEquals(Collections.emptyList(),
                myState.findCandidateServers(ReadPreference.secondary()));

        s2.update(SECONDARY_UPDATE);
        assertEquals(Collections.singletonList(s2),
                myState.findCandidateServers(ReadPreference.secondary()));

        s3.update(SECONDARY_UPDATE);
        assertEquals(
                new HashSet<Server>(Arrays.asList(s2, s3)),
                new HashSet<Server>(myState.findCandidateServers(ReadPreference
                        .secondary())));

        s2.update(PRIMARY_UPDATE);
        assertEquals(Collections.singletonList(s3),
                myState.findCandidateServers(ReadPreference.secondary()));

        s1.update(SECONDARY_UPDATE);
        s3.update(PRIMARY_UPDATE);
        assertEquals(Collections.singletonList(s1),
                myState.findCandidateServers(ReadPreference.secondary()));

        // Exclude on tags.
        s1.update(SECONDARY_UPDATE);
        s2.update(SECONDARY_UPDATE);
        s3.update(SECONDARY_UPDATE);

        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset()
                .add("tags", BuilderFactory.start().addInteger("A", 1).build())
                .build());
        final List<Server> servers = myState
                .findCandidateServers(ReadPreference.secondary(F_TAGS, G_TAGS));
        assertEquals(1, servers.size());
        assertEquals(s1, servers.get(0));

    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondaryPreferred() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        s1.update(PRIMARY_UPDATE);
        s2.update(PRIMARY_UPDATE);
        s3.update(SECONDARY_UPDATE);

        List<Server> servers = myState.findCandidateServers(ReadPreference
                .preferSecondary());
        assertEquals(3, servers.size());
        assertEquals(s3, servers.get(0)); // Non-Writable first.
        assertEquals(new HashSet<Server>(Arrays.asList(s1, s2)),
                new HashSet<Server>(servers.subList(1, servers.size())));

        // Exclude on tags.
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferSecondary(
                F_TAGS, G_TAGS));
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0));
        assertEquals(s1, servers.get(1));

        // Exclude all on tags.
        s1.update(myBuilder.reset().add("tags", F_TAGS).build());
        s3.update(myBuilder.reset().add("tags", G_TAGS).build());
        servers = myState.findCandidateServers(ReadPreference.preferSecondary(
                BuilderFactory.start().addInteger("Z", 1).build(),
                BuilderFactory.start().addInteger("Y", 1).build()));
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersSecondaryPreferredWithVeryLateServer() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

        s1.updateAverageLatency(1);
        s2.updateAverageLatency(10);
        s3.updateAverageLatency(100);

        // Mark s1 as too old.
        final long now = System.currentTimeMillis();
        myBuilder.reset().add("myState", Server.SECONDARY_STATE);
        final ArrayBuilder members = myBuilder.pushArray("members");
        members.push().add("name", s1.getCanonicalName())
                .add("optimeDate", new Date(now - TimeUnit.HOURS.toMillis(1)));
        members.push().add("name", s2.getCanonicalName())
                .add("optimeDate", new Date(now));
        members.push()
                .add("name", s3.getCanonicalName())
                .add("optimeDate", new Date(now - TimeUnit.SECONDS.toMillis(1)));
        s1.update(myBuilder.build());

        // And the rest.
        s1.update(SECONDARY_UPDATE);
        s2.update(PRIMARY_UPDATE);
        s3.update(SECONDARY_UPDATE);

        final List<Server> servers = myState
                .findCandidateServers(ReadPreference.preferSecondary());
        assertEquals(2, servers.size());
        assertEquals(s3, servers.get(0)); // Non-Writable first.
        assertEquals(new HashSet<Server>(Arrays.asList(s2)),
                new HashSet<Server>(servers.subList(1, servers.size())));
    }

    /**
     * Test method for {@link Cluster#findCandidateServers}.
     */
    @Test
    public void testFindCandidateServersServer() {
        myState = new Cluster(new MongoClientConfiguration());
        final Server s1 = myState.add("localhost:27017");
        final Server s2 = myState.add("localhost:27018");
        final Server s3 = myState.add("localhost:27019");

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
     * Test method for {@link Cluster#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        myState = new Cluster(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        myState.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final Server ss = myState.add("foo");
        assertEquals("foo:27017", ss.getCanonicalName());
        assertEquals(Server.DEFAULT_PORT, ss.getAddresses().iterator().next()
                .getPort());

        assertSame(ss, myState.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link Cluster#getNonWritableServers()}.
     */
    @Test
    public void testGetNonWritableServers() {
        myState = new Cluster(new MongoClientConfiguration());

        final Server ss = myState.add("foo");
        assertEquals("foo:27017", ss.getCanonicalName());
        assertEquals(Server.DEFAULT_PORT, ss.getAddresses().iterator().next()
                .getPort());

        ss.update(PRIMARY_UPDATE);
        assertTrue(ss.isWritable());

        assertEquals(Collections.singletonList(ss), myState.getServers());
        assertEquals(Collections.singletonList(ss),
                myState.getWritableServers());
        assertEquals(0, myState.getNonWritableServers().size());

        ss.update(SECONDARY_UPDATE);
        assertFalse(ss.isWritable());

        assertEquals(Collections.singletonList(ss), myState.getServers());
        assertEquals(Collections.singletonList(ss),
                myState.getNonWritableServers());
        assertEquals(0, myState.getWritableServers().size());

    }

    /**
     * Test method for ensuring the cluster hears when a server becomes not
     * writable.
     */
    @Test
    public void testMarkNotWritable() {
        myState = new Cluster(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final Server ss = myState.add("foo");
        assertEquals("foo:27017", ss.getCanonicalName());
        assertEquals(Server.DEFAULT_PORT, ss.getAddresses().iterator().next()
                .getPort());

        ss.update(PRIMARY_UPDATE);

        assertTrue(ss.isWritable());

        myState.addListener(mockListener);

        ss.update(SECONDARY_UPDATE);
        assertFalse(ss.isWritable());

        ss.update(SECONDARY_UPDATE);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertEquals(Boolean.TRUE, event.getValue().getOldValue());
        assertEquals(Boolean.FALSE, event.getValue().getNewValue());
    }

    /**
     * Test method for ensuring the cluster hears when a server becomes
     * writable.
     */
    @Test
    public void testMarkWritable() {
        myState = new Cluster(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final Server ss = myState.add("foo");
        assertEquals("foo:27017", ss.getCanonicalName());
        assertEquals(Server.DEFAULT_PORT, ss.getAddresses().iterator().next()
                .getPort());

        ss.update(SECONDARY_UPDATE);

        assertFalse(ss.isWritable());

        myState.addListener(mockListener);

        ss.update(PRIMARY_UPDATE);
        assertTrue(ss.isWritable());
        ss.update(PRIMARY_UPDATE);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link Cluster#removeListener(PropertyChangeListener)}.
     */
    @Test
    public void testRemoveListener() {
        myState = new Cluster(new MongoClientConfiguration());

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final Server ss = myState.add("foo");
        assertEquals("foo:27017", ss.getCanonicalName());
        assertEquals(Server.DEFAULT_PORT, ss.getAddresses().iterator().next()
                .getPort());

        ss.update(SECONDARY_UPDATE);

        assertFalse(ss.isWritable());

        myState.addListener(mockListener);

        ss.update(PRIMARY_UPDATE);
        assertTrue(ss.isWritable());
        ss.update(PRIMARY_UPDATE);

        myState.removeListener(mockListener);

        ss.update(SECONDARY_UPDATE);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(myState, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link Cluster#sort(List)}.
     */
    @Test
    public void testSortListServer() {

        final int count = 1000;
        final List<Server> servers = new ArrayList<Server>(count);
        for (int i = 0; i < count; i++) {

            final Server server = new Server(new InetSocketAddress(
                    "localhost:", i + 1024));
            server.updateAverageLatency(Math.round(Math.random() * 100000));

            servers.add(server);
        }

        myState = new Cluster(new MongoClientConfiguration());
        for (int i = 0; i < 100; ++i) {
            Collections.shuffle(servers);

            myState.sort(servers);

            // Verify that the list is sorted EXCEPT the first server.
            final double last = Double.NEGATIVE_INFINITY;
            double lowest = Double.MAX_VALUE;
            for (final Server server : servers.subList(1, count)) {
                lowest = Math.min(lowest, server.getAverageLatency());
                assertTrue(
                        "Latencies out of order: " + last + " !< "
                                + server.getAverageLatency(),
                        last < server.getAverageLatency());
            }
        }
    }
}
