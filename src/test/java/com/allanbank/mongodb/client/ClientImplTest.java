/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.MockMongoDBServer;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * ClientImplTest provides tests for the {@link ClientImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class ClientImplTest {
    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer;

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        ourServer = new MockMongoDBServer();
        ourServer.start();
    }

    /**
     * Stops a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to stop the Mock MongoDB server.
     */
    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        ourServer.setRunning(false);
        ourServer.close();
        ourServer = null;
    }

    /** The active configuration. */
    private MongoDbConfiguration myConfig;

    /** A mock connection factory. */
    private ConnectionFactory myMockConnectionFactory;

    /** The instance under test. */
    private ClientImpl myTestInstance;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockConnectionFactory = EasyMock.createMock(ConnectionFactory.class);

        myConfig = new MongoDbConfiguration();
        myTestInstance = new ClientImpl(myConfig, myMockConnectionFactory);
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockConnectionFactory = null;

        myConfig = null;
        myTestInstance = null;
        ourServer.clear();
    }

    /**
     * Test method for {@link ClientImpl#close()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testClose() throws IOException {

        final Command message = new Command("testDb", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        myMockConnectionFactory.close();
        expectLastCall();

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        mockConnection.shutdown();
        expectLastCall();

        mockConnection.waitForClosed(myConfig.getReadTimeout(),
                TimeUnit.MILLISECONDS);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(false);

        myMockConnectionFactory.close();
        expectLastCall();

        replay(mockConnection);

        myTestInstance.close();
        myTestInstance.send(message, null);
        myTestInstance.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#close()}.
     * 
     * @throws IOException
     *             on aa test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testCloseFails() throws IOException {

        final Command message = new Command("testDb", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        myMockConnectionFactory.close();
        expectLastCall();

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        mockConnection.shutdown();
        expectLastCall();

        mockConnection.waitForClosed(myConfig.getReadTimeout(),
                TimeUnit.MILLISECONDS);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(true);
        mockConnection.close();
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        myMockConnectionFactory.close();
        expectLastCall();

        replay(mockConnection);

        myTestInstance.close();
        myTestInstance.send(message, null);
        myTestInstance.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#getClusterType()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testGetClusterType() throws IOException {

        expect(myMockConnectionFactory.getClusterType()).andReturn(
                ClusterType.STAND_ALONE);

        replay();

        assertEquals(ClusterType.STAND_ALONE, myTestInstance.getClusterType());

        verify();
    }

    /**
     * Test method for {@link ClientImpl#getConfig()}.
     */
    @Test
    public void testGetConfig() {
        assertSame(myConfig, myTestInstance.getConfig());
    }

    /**
     * Test method for {@link ClientImpl#getDefaultDurability()}.
     */
    @Test
    public void testGetDefaultDurability() {
        assertSame(myConfig.getDefaultDurability(),
                myTestInstance.getDefaultDurability());
        myConfig.setDefaultDurability(Durability.journalDurable(1000));
        assertSame(myConfig.getDefaultDurability(),
                myTestInstance.getDefaultDurability());
    }

    /**
     * Test method for reconnect logic.
     */
    @Test
    public void testReconnect() {

        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        ourServer.setReplies(
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final GetLastError message = new GetLastError("testDb", Durability.ACK);
        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://"
                        + ServerNameUtils.normalize(ourServer
                                .getInetSocketAddress()));
        config.setAutoDiscoverServers(false);

        try {
            myTestInstance = new ClientImpl(config,
                    new SocketConnectionFactory(config));

            myTestInstance.send(message, null);
            ourServer.waitForRequest(1, 10000);

            ourServer.disconnectClient();
            assertTrue(ourServer.waitForDisconnect(10000));

            assertTrue(ourServer.waitForClient(100000000));
            ourServer.waitForRequest(2, 10000); // ping.

            // Give a pause for the reconnect to finish on our side.
            Thread.sleep(50);

            myTestInstance.send(message, null);
            ourServer.waitForRequest(3, 10000);
        }
        catch (final InterruptedException e) {
            // Ignore.
        }
        finally {
            myTestInstance.close();
        }

    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendGetMoreCallbackOfReply() throws IOException {

        final Callback<Reply> callback = createMock(Callback.class);
        final GetMore message = new GetMore("testDb", "collection", 1234L,
                12345, ReadPreference.PRIMARY);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, callback)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, callback);

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessage() throws IOException {
        final Update message = new Update("testDb", "collection", null, null,
                false, false);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, null);

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessageAndCreateConnectionFailes() throws IOException {
        final Update message = new Update("testDb", "collection", null, null,
                false, false);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andThrow(new IOException());

        replay(mockConnection);

        try {
            myTestInstance.send(message, null);
            fail("Should have thrown a MongoDbException.");
        }
        catch (final MongoDbException good) {
            // good.
        }

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageClosesFirstWhenMaxShrinks() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        mockConnection.shutdown();
        expectLastCall();

        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(0);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection, mockConnection2);

        myConfig.setMaxConnectionCount(2);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myConfig.setMaxConnectionCount(1);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageClosesFirstWhenMaxShrinksAndCloseFails()
            throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        mockConnection.shutdown();
        expectLastCall();

        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(0);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection, mockConnection2);

        myConfig.setMaxConnectionCount(2);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myConfig.setMaxConnectionCount(1);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageCreatesSecondConnectionOnPending()
            throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessageGetLastErrorCallbackOfReply() throws IOException {
        final Message message = new Update("testDb", "collection", null, null,
                false, false);
        final GetLastError lastError = new GetLastError("testDb", false, false,
                0, 0);
        final Callback<Reply> callback = createMock(Callback.class);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, lastError, callback)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, lastError, callback);

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessagePicksIdleExisting() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));
        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(0);
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

        verify(mockConnection);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessagePicksMostIdleWhenAllPending() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        // First pass for idle.
        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(2);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        // Now most idle.
        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(2);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendQueryCallbackOfReply() throws IOException {
        final Query message = new Query("db", "coll", null, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);
        final Callback<Reply> callback = createMock(Callback.class);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, callback)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, callback);

        verify(mockConnection);
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockConnectionFactory);
    }

    /**
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockConnectionFactory);
    }
}
