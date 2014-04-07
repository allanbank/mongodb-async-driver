/*
 * Copyright 2012-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;

import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ConnectionInfo;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;

/**
 * ReplicaSetConnectionTest provides tests of the {@link ReplicaSetConnection}.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
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

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (final Thread t : threads) {
            if (t != null) {
                if (t.getName().contains("<--")) {
                    assertThat("Found receive threads: " + t.getName(),
                            t.isAlive(), is(false));
                }
            }
        }
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testClosePrimaryReconnect() throws IOException {

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReplicaSetReconnectStrategy mockStrategy = createMock(ReplicaSetReconnectStrategy.class);
        final Capture<PropertyChangeListener> listenerCapture = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listenerCapture));
        expectLastCall();

        // On the closed property change.
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.raiseErrors(anyObject(MongoDbException.class));
        expectLastCall();
        expect(mockConnection.isShuttingDown()).andReturn(false);
        mockConnection.shutdown(true);
        expectLastCall();

        expect(mockStrategy.reconnectPrimary()).andReturn(
                new ConnectionInfo<Server>(mockConnection2, myServer));
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                mockStrategy);

        listenerCapture.getValue().propertyChange(
                new PropertyChangeEvent(mockConnection,
                        Connection.OPEN_PROP_NAME, true, false));

        verify(mockConnection, mockFactory, mockConnection2, mockStrategy);

        //
        // Stage 2: The close should only close the new connection.
        //
        reset(mockConnection, mockFactory, mockConnection2, mockStrategy);

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockStrategy);
        testConnection.close();
        verify(mockConnection, mockFactory, mockConnection2, mockStrategy);

    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testClosePrimaryReconnectFails() throws IOException {

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReplicaSetReconnectStrategy mockStrategy = createMock(ReplicaSetReconnectStrategy.class);
        final Capture<PropertyChangeListener> listenerCapture = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listenerCapture));
        expectLastCall();

        // On the closed property change.
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.raiseErrors(anyObject(MongoDbException.class));
        expectLastCall();
        expect(mockConnection.isShuttingDown()).andReturn(false);
        mockConnection.shutdown(true);
        expectLastCall();

        expect(mockStrategy.reconnectPrimary()).andReturn(null);

        replay(mockConnection, mockFactory, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                mockStrategy);

        listenerCapture.getValue().propertyChange(
                new PropertyChangeEvent(mockConnection,
                        Connection.OPEN_PROP_NAME, true, false));

        verify(mockConnection, mockFactory, mockStrategy);

        //
        // Stage 2: The close should only close the new connection.
        //
        reset(mockConnection, mockFactory, mockStrategy);

        replay(mockConnection, mockFactory, mockStrategy);
        testConnection.close();
        verify(mockConnection, mockFactory, mockStrategy);

    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testClosePrimaryReconnectFailsWithASecondaryConnection()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final Server s1 = myCluster.add("foo:12345");
        s1.updateAverageLatency(1000);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReplicaSetReconnectStrategy mockStrategy = createMock(ReplicaSetReconnectStrategy.class);
        final Capture<PropertyChangeListener> listenerCapture = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listenerCapture));
        expectLastCall();

        expect(mockFactory.connect(s1, myConfig)).andReturn(mockConnection2);
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        // On the closed property change.
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.raiseErrors(anyObject(MongoDbException.class));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        expect(mockStrategy.reconnectPrimary()).andReturn(null);

        replay(mockConnection, mockConnection2, mockFactory, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                mockStrategy);

        // Open the connection.
        testConnection.send(q, null);

        listenerCapture.getValue().propertyChange(
                new PropertyChangeEvent(mockConnection,
                        Connection.OPEN_PROP_NAME, true, false));

        verify(mockConnection, mockConnection2, mockFactory, mockStrategy);

        //
        // Stage 2: The close should only close the new connection.
        //
        reset(mockConnection, mockConnection2, mockFactory, mockStrategy);

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockConnection2, mockFactory, mockStrategy);
        testConnection.close();
        verify(mockConnection, mockConnection2, mockFactory, mockStrategy);

    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

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
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        // Open the connection.
        testConnection.send(q, null);

        // Give the s6 server the same name as once of the other servers.
        // That will cause the connection to be shutdown.
        s6.update(BuilderFactory.start().add("me", s1.getCanonicalName())
                .build());

        verify(mockConnection, mockFactory, mockConnection2);

        // The close should only close the main connection.
        reset(mockConnection, mockFactory, mockConnection2);

        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);
        testConnection.close();
        verify(mockConnection, mockFactory, mockConnection2);

    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall().andThrow(new IOException("Injected"));

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection.flush();
        expectLastCall();
        mockConnection2.flush();
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);

        testConnection.flush();
        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection.flush();
        expectLastCall();
        mockConnection2.flush();
        expectLastCall().andThrow(new IOException("Injected"));

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);

        testConnection.flush();
        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testIgnoreServerAdd() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        myCluster.add("foo:12345");

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSend() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Message msg = new IsMaster();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.isAvailable()).andReturn(true);
        mockConnection.send(eq(msg), isNull(ReplyCallback.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(msg, null);

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSend2Messages() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Message msg = new IsMaster();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.isAvailable()).andReturn(true);
        mockConnection.send(eq(msg), eq(msg), isNull(ReplyCallback.class));
        expectLastCall();

        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(msg, msg, null);

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

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
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
     *
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendNoMessages() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);
        try {
            testConnection.send(null, null);
            fail("Should have complained that it could not find a server.");
        }
        catch (final MongoDbException good) {
            assertThat(good.getMessage(),
                    containsString("Could not find any servers "
                            + "for the following set of read preferences: ."));
        }
        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        try {
            testConnection.send(q1, q3, null);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertThat(good.getMessage(), containsString(q1.getReadPreference()
                    .toString()));
            assertThat(good.getMessage(), containsString(q3.getReadPreference()
                    .toString()));

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
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, q, null);
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, q, null);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

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
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();
        expect(mockConnection2.isAvailable()).andReturn(true);
        mockConnection2.send(q, null);
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);
        testConnection.send(q, null);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(q, null);
        expectLastCall();

        // Second message.
        expect(mockConnection2.isAvailable()).andReturn(false);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        mockConnection3
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(
                mockConnection3);

        mockConnection2
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.shutdown(true);
        expectLastCall();

        mockConnection3.send(q, null);
        expectLastCall();

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection3
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection3.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockStrategy,
                mockConnection3);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        testConnection.send(q, null);
        testConnection.send(q, null);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockStrategy,
                mockConnection3);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

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
     * Test method for {@link ReplicaSetConnection#send(Message, ReplyCallback)}
     * .
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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

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

        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig,
                null);

        assertEquals("ReplicaSet(" + mockConnection.toString() + ")",
                testConnection.toString());

        testConnection.close();

        verify(mockConnection, mockFactory);
    }
}
