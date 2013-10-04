/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;

/**
 * ReplicaSetConnectionTest provides tests of the {@link ReplicaSetConnection}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionTest {

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

    /** Update document to mark servers as the secondary. */
    private static final Document SECONDARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", false)
                    .add("secondary", true));

    /** The cluster being used in the test. */
    private Cluster myCluster;

    /** The configuration being used in the test. */
    private MongoClientConfiguration myConfig;

    /** The test primary server. */
    private Server myServer;

    /**
     * Creates test state.
     */
    @Before
    public void setUp() {
        myConfig = new MongoClientConfiguration();
        myCluster = new Cluster(myConfig);

        myServer = myCluster.get("localhost:27017");
        myServer.update(PRIMARY_UPDATE);
    }

    /**
     * Cleans up the test state.
     */
    @After
    public void tearDown() {
        myCluster = null;
        myConfig = null;
        myServer = null;
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testCloseSecondaryOnRemoveFromTheCluster() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        mockConnection2.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        // Open the connection.
        assertEquals("foo", testConnection.send(q, null));

        // Give the s6 server the same name as once of the other servers.
        // That will cause the connection to be shutdown.
        s6.update(BuilderFactory.start().add("me", s1.getCanonicalName())
                .build());

        verify(mockConnection, mockFactory, mockConnection2);

        // The close should only close the main connection.
        reset(mockConnection, mockFactory, mockConnection2);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);
        testConnection.close();
        verify(mockConnection, mockFactory, mockConnection2);

    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testCloseSecondaryThrowsIOException() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall().andThrow(new IOException("Injected"));

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testFlushSecondary() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        mockConnection.flush();
        expectLastCall();
        mockConnection2.flush();
        expectLastCall();

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));

        testConnection.flush();
        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testFlushSecondaryThrowsIOException() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        mockConnection.flush();
        expectLastCall();
        mockConnection2.flush();
        expectLastCall().andThrow(new IOException("Injected"));

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));

        testConnection.flush();
        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testIgnoreServerAdd() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        myCluster.add("foo:12345");

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSend() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Message msg = new IsMaster();

        expect(mockConnection.send(eq(msg), isNull(Callback.class))).andReturn(
                "foo");
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(msg, null));

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSend2Messages() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Message msg = new IsMaster();

        expect(mockConnection.send(eq(msg), eq(msg), isNull(Callback.class)))
                .andReturn("foo");
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(msg, msg, null));

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendNoAvailableServerForReadPreferences()
            throws IOException {

        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.server("foo:12345"),
                false, false, false, false);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(q, null);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertTrue(good.getMessage().contains(
                    q.getReadPreference().toString()));
        }
        finally {
            testConnection.close();
        }

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendNoMessages() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockConnection.send(null, null)).andReturn("foo");
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(null, null));

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendOnlyListsAPreferenceOnce() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Query q1 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.primary(), false, false,
                false, false);
        final Query q3 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(q1, q3, null);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertTrue(good.getMessage().contains(
                    q1.getReadPreference().toString()));
            assertTrue(good.getMessage().contains(
                    q3.getReadPreference().toString()));

            final int indexof = good.getMessage().indexOf(
                    q1.getReadPreference().toString());
            assertTrue(good.getMessage().indexOf(
                    q1.getReadPreference().toString(), indexof + 1) < 0);
        }
        finally {
            testConnection.close();
        }

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendToSecondary2Messages() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, q, null)).andReturn("foo");

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, q, null));

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendToSecondaryAllFail() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");

        s1.update(SECONDARY_UPDATE);
        s2.update(SECONDARY_UPDATE);
        s3.update(SECONDARY_UPDATE);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(q, null);
            fail("Should have failed completely.");
        }
        catch (final MongoDbException error) {
            // Good.
        }
        finally {
            testConnection.close();
        }

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendToSecondaryNoOpenConnection() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryReuseConnectionOnSecondSend()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.send(q, null)).andReturn("bar");

        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));
        assertEquals("bar", testConnection.send(q, null));

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryReuseConnectionOnSecondSendWithReconnect()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        final Server s2 = myCluster.add("bar:12345");
        final Server s3 = myCluster.add("bas:12345");
        final Server s4 = myCluster.add("bat:12345");
        final Server s5 = myCluster.add("bau:12345");
        final Server s6 = myCluster.add("bav:12345");

        s1.updateAverageLatency(1000);
        s2.updateAverageLatency(1000);
        s3.updateAverageLatency(1000);
        s4.updateAverageLatency(1000);
        s5.updateAverageLatency(1000);
        s6.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(mockFactory.connect(s1, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s2, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s3, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s4, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);
        expect(mockFactory.connect(s5, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 2);

        expect(mockFactory.connect(s6, myConfig)).andReturn(mockConnection2);
        expect(mockConnection2.send(q, null)).andReturn("foo");

        // Second message.
        expect(mockConnection2.isOpen()).andReturn(false);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(
                mockConnection3);

        mockConnection2.close();
        expectLastCall();

        expect(mockConnection3.send(q, null)).andReturn("bar");

        mockConnection.close();
        expectLastCall();
        mockConnection3.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockStrategy,
                mockConnection3);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(q, null));
        assertEquals("bar", testConnection.send(q, null));

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockStrategy,
                mockConnection3);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendWithReadPreferencesConflict() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Query q1 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.primary(), false, false,
                false, false);
        final Query q2 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(q1, q2, null);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertTrue(good.getMessage().contains(
                    q1.getReadPreference().toString()));
            assertTrue(good.getMessage().contains(
                    q2.getReadPreference().toString()));
        }
        finally {
            testConnection.close();
        }

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendWithReadPreferencesConflictRemoveDups()
            throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Query q1 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);
        final Query q2 = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(q1, q2, null);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertTrue(good.getMessage().contains(
                    q1.getReadPreference().toString()));
            assertTrue(good.getMessage().contains(
                    q2.getReadPreference().toString()));
        }
        finally {
            testConnection.close();
        }

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#toString()}.
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testToString() throws IOException {

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("ReplicaSet(" + mockConnection.toString() + ")",
                testConnection.toString());

        testConnection.close();

        verify(mockConnection, mockFactory);
    }
}
