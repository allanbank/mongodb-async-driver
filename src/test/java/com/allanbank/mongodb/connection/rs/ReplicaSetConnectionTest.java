/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.CallbackReply;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * ReplicaSetConnectionTest provides tests of the {@link ReplicaSetConnection}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionTest {

    /** The cluster being used in the test. */
    private ClusterState myCluster;

    /** The configuration being used in the test. */
    private MongoDbConfiguration myConfig;

    /** The test primary server. */
    private ServerState myServer;

    /**
     * Creates test state.
     */
    @Before
    public void setUp() {
        myCluster = new ClusterState();
        myConfig = new MongoDbConfiguration();

        myServer = myCluster.get("localhost:27017");
        myCluster.markWritable(myServer);
    }

    /**
     * Cleans up the test state.
     */
    @After
    public void tearDown() {
        myCluster.close();
        myCluster = null;
        myConfig = null;
        myServer = null;
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
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

        expect(mockConnection.send(isNull(Callback.class), eq(msg))).andReturn(
                "foo");
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(null, msg));

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
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
            testConnection.send(null, q);
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
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendNoMessages() throws IOException {
        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockConnection.send(null, new Message[0])).andReturn("foo");
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(null));

        testConnection.close();

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
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
            testConnection.send(null, q1, q2, q3);
            fail("Should not have found any available server for the read preference.");
        }
        catch (final MongoDbException good) {
            assertTrue(good.getMessage().contains(
                    q1.getReadPreference().toString()));
            assertTrue(good.getMessage().contains(
                    q2.getReadPreference().toString()));

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
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendSecondMessageRemovesServer() throws IOException {
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
            testConnection.send(null, q1, q2);
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
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendToSecondaryAllFail() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState s1 = myCluster.add("foo:12345");
        final ServerState s2 = myCluster.add("bar:12345");
        final ServerState s3 = myCluster.add("bas:12345");
        final ServerState s4 = myCluster.add("bat:12345");
        final ServerState s5 = myCluster.add("bau:12345");
        final ServerState s6 = myCluster.add("bav:12345");

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
        expect(mockFactory.connect(s6, myConfig)).andThrow(
                new IOException("Oops.")).times(0, 1);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        try {
            testConnection.send(null, q);
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
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @Test
    public void testSendToSecondaryNoOpenConnection() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState s1 = myCluster.add("foo:12345");
        final ServerState s2 = myCluster.add("bar:12345");
        final ServerState s3 = myCluster.add("bas:12345");
        final ServerState s4 = myCluster.add("bat:12345");
        final ServerState s5 = myCluster.add("bau:12345");
        final ServerState s6 = myCluster.add("bav:12345");

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
        expect(mockConnection2.send(null, q)).andReturn("foo");

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        assertEquals("foo", testConnection.send(null, q));
        assertSame(mockConnection2, s6.takeConnection());

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryNoOpenConnectionSecondTake()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState s1 = myCluster.add("foo:12345");
        final ServerState s2 = myCluster.add("bar:12345");
        final ServerState s3 = myCluster.add("bas:12345");
        final ServerState s4 = myCluster.add("bat:12345");
        final ServerState s5 = myCluster.add("bau:12345");
        final ServerState s6 = myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final Connection mockConnection4 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.FALSE);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(null);
        mockConnection2.close();
        expectLastCall();

        final IAnswer<Connection> factoryAnswer = new IAnswer<Connection>() {
            @Override
            public Connection answer() throws Throwable {
                s1.addConnection(mockConnection3);
                s2.addConnection(mockConnection3);
                s3.addConnection(mockConnection3);
                s4.addConnection(mockConnection3);
                s5.addConnection(mockConnection3);
                s6.addConnection(mockConnection3);
                throw new IOException("Inject");
            }
        };

        expect(mockFactory.connect(s1, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s6, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);

        expect(mockConnection3.isOpen()).andReturn(Boolean.TRUE);
        expect(mockConnection3.send(null, q)).andReturn("foo");

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        s1.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(null, q));

        for (final ServerState s : Arrays.asList(s1, s2, s3, s4, s5, s6)) {
            assertSame(mockConnection3, s.takeConnection());
        }

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryNoOpenConnectionSecondTakeButCannotGiveBack()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState s1 = myCluster.add("foo:12345");
        final ServerState s2 = myCluster.add("bar:12345");
        final ServerState s3 = myCluster.add("bas:12345");
        final ServerState s4 = myCluster.add("bat:12345");
        final ServerState s5 = myCluster.add("bau:12345");
        final ServerState s6 = myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final Connection mockConnection4 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.FALSE);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(null);
        mockConnection2.close();
        expectLastCall();

        final IAnswer<Connection> factoryAnswer = new IAnswer<Connection>() {
            @Override
            public Connection answer() throws Throwable {
                s1.addConnection(mockConnection3);
                s2.addConnection(mockConnection3);
                s3.addConnection(mockConnection3);
                s4.addConnection(mockConnection3);
                s5.addConnection(mockConnection3);
                s6.addConnection(mockConnection3);
                throw new IOException("Inject");
            }
        };

        expect(mockFactory.connect(s1, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s6, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);

        expect(mockConnection3.isOpen()).andReturn(Boolean.TRUE);
        expect(
                mockConnection3.send(
                        cbAndSetConn(mockConnection3, s1, s2, s3, s4, s5, s6),
                        eq(q))).andReturn("foo");
        mockConnection3.shutdown();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        s1.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(new FutureCallback<Reply>(), q));

        for (final ServerState s : Arrays.asList(s1, s2, s3, s4, s5, s6)) {
            assertSame(mockConnection3, s.takeConnection());
        }

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryNoOpenConnectionSecondTakeReconnect()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState s1 = myCluster.add("foo:12345");
        final ServerState s2 = myCluster.add("bar:12345");
        final ServerState s3 = myCluster.add("bas:12345");
        final ServerState s4 = myCluster.add("bat:12345");
        final ServerState s5 = myCluster.add("bau:12345");
        final ServerState s6 = myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final Connection mockConnection4 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.FALSE);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(null);
        mockConnection2.close();
        expectLastCall();

        final IAnswer<Connection> factoryAnswer = new IAnswer<Connection>() {
            @Override
            public Connection answer() throws Throwable {
                s1.addConnection(mockConnection3);
                s2.addConnection(mockConnection3);
                s3.addConnection(mockConnection3);
                s4.addConnection(mockConnection3);
                s5.addConnection(mockConnection3);
                s6.addConnection(mockConnection3);
                throw new IOException("Inject");
            }
        };

        expect(mockFactory.connect(s1, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s2, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s3, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s4, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s5, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);
        expect(mockFactory.connect(s6, myConfig)).andAnswer(factoryAnswer)
                .times(0, 1);

        expect(mockConnection3.isOpen()).andReturn(Boolean.FALSE);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection3)).andReturn(
                mockConnection4);
        mockConnection3.close();
        expectLastCall();
        expect(mockConnection4.send(null, q)).andReturn("foo");

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        s1.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(null, q));

        boolean found = false;
        for (final ServerState s : Arrays.asList(s1, s2, s3, s4, s5, s6)) {
            final Connection c = s.takeConnection();
            if (c == mockConnection4) {
                found = true;
            }
        }
        assertTrue(found);

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockConnection4, mockStrategy);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryWithOpenConnection() throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState second = myCluster.add("foo:12345");
        myCluster.add("bar:12345");
        myCluster.add("bas:12345");
        myCluster.add("bat:12345");
        myCluster.add("bau:12345");
        myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.TRUE);
        expect(mockConnection2.send(null, q)).andReturn("foo");

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        second.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(null, q));
        assertSame(mockConnection2, second.takeConnection());

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryWithOpenConnectionButCannotGiveBack()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState second = myCluster.add("foo:12345");
        myCluster.add("bar:12345");
        myCluster.add("bas:12345");
        myCluster.add("bat:12345");
        myCluster.add("bau:12345");
        myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.TRUE);
        expect(
                mockConnection2.send(cbAndSetConn(mockConnection, second),
                        eq(q))).andReturn("foo");

        mockConnection2.shutdown();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        second.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(new FutureCallback<Reply>(), q));
        assertSame(mockConnection, second.takeConnection());

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2);
    }

    /**
     * Test method for {@link ReplicaSetConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up mocks.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendToSecondaryWithOpenConnectionReconnect()
            throws IOException {
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.secondary(), false, false,
                false, false);

        final ServerState second = myCluster.add("foo:12345");
        myCluster.add("bar:12345");
        myCluster.add("bas:12345");
        myCluster.add("bat:12345");
        myCluster.add("bau:12345");
        myCluster.add("bav:12345");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(mockConnection2.isOpen()).andReturn(Boolean.FALSE);
        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        expect(mockStrategy.reconnect(mockConnection2)).andReturn(
                mockConnection3);
        mockConnection2.close();
        expectLastCall();
        expect(mockConnection3.send(null, q)).andReturn("foo");

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockStrategy);

        final ReplicaSetConnection testConnection = new ReplicaSetConnection(
                mockConnection, myServer, myCluster, mockFactory, myConfig);

        second.addConnection(mockConnection2);
        assertEquals("foo", testConnection.send(null, q));
        assertSame(mockConnection3, second.takeConnection());

        testConnection.close();

        verify(mockConnection, mockFactory, mockConnection2, mockConnection3,
                mockStrategy);
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

    /**
     * Creates a new CallbackReply.
     * 
     * @param conn
     *            The connection to give the server.
     * @param states
     *            The states to give the connection to.
     * 
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndSetConn(final Connection conn,
            final ServerState... states) {
        class CallbackWithSetConnection extends CallbackReply {

            private static final long serialVersionUID = -2458416861114720698L;

            public CallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
                for (final ServerState state : states) {
                    state.addConnection(conn);
                }
            }
        }
        EasyMock.capture(new CallbackWithSetConnection(CallbackReply.reply()));
        return null;
    }
}
