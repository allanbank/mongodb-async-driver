/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.MockMongoDBServer;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerSelector;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.connection.state.SimpleReconnectStrategy;
import com.allanbank.mongodb.error.CannotConnectException;
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

    /** The instance under test. */
    protected ClientImpl myTestInstance;

    /** The active configuration. */
    private MongoClientConfiguration myConfig;

    /** A mock connection factory. */
    private ProxiedConnectionFactory myMockConnectionFactory;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockConnectionFactory = EasyMock
                .createMock(ProxiedConnectionFactory.class);

        myConfig = new MongoClientConfiguration();
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
     * Test method for {@link ClientImpl#close()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testCloseOnThrownIoException() throws IOException {

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
        expectLastCall().andThrow(new IOException("This is a test."));
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
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testCreatesConnectionOnScannedPending() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(7);

        final Connection mockConnection1 = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final Connection mockConnection3 = createMock(Connection.class);
        final Connection mockConnection4 = createMock(Connection.class);
        final Connection mockConnection5 = createMock(Connection.class);
        final Connection mockConnection6 = createMock(Connection.class);
        final Connection mockConnection7 = createMock(Connection.class);

        // First request - start at sequence zero.
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection1);
        mockConnection1
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection1.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Second request - Still at sequence zero.
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Third Request - start at sequence 1.
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection3);
        mockConnection3
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection3.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Fourth Request - start at sequence 3.
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection3.isOpen()).andReturn(true);
        expect(mockConnection3.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection4);
        mockConnection4
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection4.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Fifth request - start at sequence 6.
        expect(mockConnection3.isOpen()).andReturn(true);
        expect(mockConnection3.getPendingCount()).andReturn(1);
        expect(mockConnection4.isOpen()).andReturn(true);
        expect(mockConnection4.getPendingCount()).andReturn(1);
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection5);
        mockConnection5
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection5.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Sixth request - start at sequence 10.
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection3.isOpen()).andReturn(true);
        expect(mockConnection3.getPendingCount()).andReturn(1);
        expect(mockConnection4.isOpen()).andReturn(true);
        expect(mockConnection4.getPendingCount()).andReturn(1);
        expect(mockConnection5.isOpen()).andReturn(true);
        expect(mockConnection5.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection6);
        mockConnection6
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection6.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Seventh Request - last connection - start at sequence 15.
        expect(mockConnection4.isOpen()).andReturn(true);
        expect(mockConnection4.getPendingCount()).andReturn(1);
        expect(mockConnection5.isOpen()).andReturn(true);
        expect(mockConnection5.getPendingCount()).andReturn(1);
        expect(mockConnection6.isOpen()).andReturn(true);
        expect(mockConnection6.getPendingCount()).andReturn(1);
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection7);
        mockConnection7
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection7.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        reset(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);

        // Eighth request - Most idle - start at sequence 20.
        // First pass for idle.
        expect(mockConnection7.isOpen()).andReturn(true);
        expect(mockConnection7.getPendingCount()).andReturn(1);
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(1);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(1);
        expect(mockConnection3.isOpen()).andReturn(true);
        expect(mockConnection3.getPendingCount()).andReturn(1);
        expect(mockConnection4.isOpen()).andReturn(true);
        expect(mockConnection4.getPendingCount()).andReturn(1);
        // Second for most idle.
        expect(mockConnection5.isOpen()).andReturn(true);
        expect(mockConnection5.getPendingCount()).andReturn(2);
        expect(mockConnection6.isOpen()).andReturn(true);
        expect(mockConnection6.getPendingCount()).andReturn(1);
        expect(mockConnection7.isOpen()).andReturn(true);
        expect(mockConnection7.getPendingCount()).andReturn(5);
        expect(mockConnection1.isOpen()).andReturn(true);
        expect(mockConnection1.getPendingCount()).andReturn(4);
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(3);
        expect(mockConnection6.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
        myTestInstance.send(message, null);
        verify(mockConnection1, mockConnection2, mockConnection3,
                mockConnection4, mockConnection5, mockConnection6,
                mockConnection7);
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
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testHandleConnectionClosedForUnknownConnection()
            throws IOException {
        final Connection mockConnection = createMock(Connection.class);

        // Response to the handleConnextionClosed.
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        replay(mockConnection);

        myTestInstance.handleConnectionClosed(mockConnection);

        verify(mockConnection);
    }

    /**
     * Test method for reconnect logic.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testInvalidPrpertyChange() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        final Capture<PropertyChangeListener> propListenerCapture = new Capture<PropertyChangeListener>();
        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection.addPropertyChangeListener(capture(propListenerCapture));
        expectLastCall();

        // First send to create the connection.
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection);

        myTestInstance.send(message, null);

        propListenerCapture.getValue().propertyChange(
                new PropertyChangeEvent(mockConnection,
                        Connection.OPEN_PROP_NAME + "g", Boolean.TRUE,
                        Boolean.FALSE));
        propListenerCapture.getValue()
                .propertyChange(
                        new PropertyChangeEvent(mockConnection,
                                Connection.OPEN_PROP_NAME, Boolean.FALSE,
                                Boolean.TRUE));
        propListenerCapture.getValue().propertyChange(
                new PropertyChangeEvent(mockConnection,
                        Connection.OPEN_PROP_NAME, Boolean.TRUE, Integer
                                .valueOf(1)));

        // Verify that the connection is not removed.
        assertEquals(1, myTestInstance.getConnectionCount());

        verify(mockConnection);

    }

    /**
     * Test method for reconnect logic.
     */
    @Test
    public void testReconnect() {

        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
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
        final MongoClientConfiguration config = new MongoClientConfiguration(
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
     * Test method for reconnect logic.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testReconnectFails() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        final Capture<PropertyChangeListener> propListenerCapture = new Capture<PropertyChangeListener>();
        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection.addPropertyChangeListener(capture(propListenerCapture));
        expectLastCall();

        // First send to create the connection.
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        // We trigger the connection failure.
        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();
        strategy.setConfig(myConfig);
        strategy.setSelector(new ServerSelector() {

            @Override
            public List<ServerState> pickServers() {
                return Collections.singletonList(new ServerState(
                        new InetSocketAddress("localhost", 27017)));
            }
        });
        strategy.setConnectionFactory(myMockConnectionFactory);

        expect(myMockConnectionFactory.getReconnectStrategy()).andReturn(
                strategy);

        // Create a new connection for the reconnect.
        mockConnection
                .raiseErrors(anyObject(MongoDbException.class), eq(false));
        expectLastCall();
        expect(
                myMockConnectionFactory.connect(anyObject(ServerState.class),
                        eq(myConfig))).andReturn(mockConnection2);

        // The ping! -- Fail.
        expect(
                mockConnection2.send(eq(new IsMaster()),
                        anyObject(Callback.class))).andThrow(
                new MongoDbException("synthetic ping error"));
        mockConnection2.close();
        expectLastCall();

        mockConnection.raiseErrors(anyObject(MongoDbException.class), eq(true));
        expectLastCall();
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);

        propListenerCapture.getValue()
                .propertyChange(
                        new PropertyChangeEvent(mockConnection,
                                Connection.OPEN_PROP_NAME, Boolean.TRUE,
                                Boolean.FALSE));

        // Verify that the connection is removed.
        assertEquals(0, myTestInstance.getConnectionCount());

        verify(mockConnection, mockConnection2);

    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     * @throws InterruptedException
     *             On a failure to pause in the test.
     */
    @Test
    public void testReconnectThatFails() throws IOException,
            InterruptedException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(1);

        final Connection mockConnection = createMock(Connection.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        // Reconnect.
        mockConnection
                .raiseErrors(anyObject(MongoDbException.class), eq(false));
        expectLastCall();
        expect(myMockConnectionFactory.getReconnectStrategy()).andReturn(
                mockStrategy);
        expect(mockStrategy.reconnect(mockConnection)).andReturn(null);
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.raiseErrors(anyObject(MongoDbException.class), eq(true));
        expectLastCall();

        replay(mockConnection, mockStrategy);

        myTestInstance.send(message, null);
        myTestInstance.handleConnectionClosed(mockConnection);

        verify(mockConnection, mockStrategy);
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRestartDocumentAssignable() throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add("ns", "a.b");
        b.add("$cursor_id", 123456);
        b.add("$server", "server");
        b.add("$limit", 4321);
        b.add("$batch_size", 23);

        final GetMore message = new GetMore("a", "b", 123456, 23,
                ReadPreference.server("server"));
        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(eq(message), anyObject(Callback.class)))
                .andReturn(
                        ServerNameUtils.normalize(ourServer
                                .getInetSocketAddress()));

        replay(mockConnection);

        final MongoIterator<Document> iter = myTestInstance.restart(b);

        verify(mockConnection);

        assertThat(iter, instanceOf(MongoIteratorImpl.class));
        final MongoIteratorImpl iterImpl = (MongoIteratorImpl) iter;
        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("a"));
        assertThat(iterImpl.getCollectionName(), is("b"));
        assertThat(iterImpl.getClient(), is((Client) myTestInstance));
        assertThat(iterImpl.getReadPerference(),
                is(ReadPreference.server("server")));
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testRestartDocumentAssignableNonCursorDoc() throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add("ns", "a.b");
        b.add("$cursor_id", 123456);
        b.add("$server", "server");
        b.add("$limit", 4321);
        b.add("$batch_size", 23);

        replay();

        // Missing fields.
        b.remove("$batch_size");
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$batch_size", 23);

        b.remove("$limit");
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$limit", 23);

        b.remove("$server");
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$server", "server");

        b.remove("$cursor_id");
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$cursor_id", 23);

        b.remove("ns");
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("ns", "a.b");

        // Too few fields.
        b.remove("$batch_size");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.add("$batch_size", 23);

        // Wrong Field type.
        b.remove("$batch_size");
        b.add("$batch_size", "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$batch_size");
        b.add("$batch_size", 23);

        b.remove("$limit");
        b.add("$limit", "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$limit");
        b.add("$limit", 23);

        b.remove("$server");
        b.add("$server", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$server");
        b.add("$server", "server");

        b.remove("$cursor_id");
        b.add("$cursor_id", "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$cursor_id");
        b.add("$cursor_id", 23);

        b.remove("ns");
        b.add("ns", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("ns");
        b.add("ns", "a.b");

        verify();

    }

    /**
     * Test method for
     * {@link ClientImpl#restart(StreamCallback, DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRestartStreamCallbackDocumentAssignable()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add("ns", "a.b");
        b.add("$cursor_id", 123456);
        b.add("$server", "server");
        b.add("$limit", 4321);
        b.add("$batch_size", 23);

        final GetMore message = new GetMore("a", "b", 123456, 23,
                ReadPreference.server("server"));
        final StreamCallback<Document> mockStreamCallback = createMock(StreamCallback.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(eq(message), anyObject(Callback.class)))
                .andReturn(
                        ServerNameUtils.normalize(ourServer
                                .getInetSocketAddress()));

        replay(mockConnection, mockStreamCallback);

        final MongoCursorControl iter = myTestInstance.restart(
                mockStreamCallback, b);

        verify(mockConnection, mockStreamCallback);

        assertThat(iter, instanceOf(QueryStreamingCallback.class));
        final QueryStreamingCallback iterImpl = (QueryStreamingCallback) iter;
        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("a"));
        assertThat(iterImpl.getCollectionName(), is("b"));
        assertThat(iterImpl.getClient(), is((Client) myTestInstance));
        assertThat(iterImpl.getAddress(), is("server"));
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testRestartStreamCallbackDocumentAssignableNonCursorDoc()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add("ns", "a.b");
        b.add("$cursor_id", 123456);
        b.add("$server", "server");
        b.add("$limit", 4321);
        b.add("$batch_size", 23);

        final StreamCallback<Document> mockStreamCallback = createMock(StreamCallback.class);

        replay(mockStreamCallback);

        // Missing fields.
        b.remove("$batch_size");
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$batch_size", 23);

        b.remove("$limit");
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$limit", 23);

        b.remove("$server");
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$server", "server");

        b.remove("$cursor_id");
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("$cursor_id", 23);

        b.remove("ns");
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add("ns", "a.b");

        // Too few fields.
        b.remove("$batch_size");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.add("$batch_size", 23);

        // Wrong Field type.
        b.remove("$batch_size");
        b.add("$batch_size", "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$batch_size");
        b.add("$batch_size", 23);

        b.remove("$limit");
        b.add("$limit", "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$limit");
        b.add("$limit", 23);

        b.remove("$server");
        b.add("$server", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$server");
        b.add("$server", "server");

        b.remove("$cursor_id");
        b.add("$cursor_id", "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("$cursor_id");
        b.add("$cursor_id", 23);

        b.remove("ns");
        b.add("ns", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("ns");
        b.add("ns", "a.b");

        verify(mockStreamCallback);

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

        // Response to the handleConnextionClosed.
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        replay(mockConnection, mockConnection2);

        myConfig.setMaxConnectionCount(2);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myConfig.setMaxConnectionCount(1);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myTestInstance.handleConnectionClosed(mockConnection);

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
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageFailsWhenAllAreClosed() throws IOException {
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
        expect(mockConnection.isOpen()).andReturn(false);
        expect(mockConnection2.isOpen()).andReturn(false);

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        try {
            myTestInstance.send(message, null);
            fail("Should have failed.");
        }
        catch (final CannotConnectException failure) {
            assertThat(
                    failure.getMessage(),
                    containsString("Could not create a connection to the server."));
        }
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
     * @throws InterruptedException
     *             On a failure to pause in the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageWaitsForReconnect() throws IOException,
            InterruptedException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(1);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        makeThreadSafe(mockConnection, mockConnection2);

        final ReconnectStrategy pauseStrategy = new SimpleReconnectStrategy() {
            @Override
            public Connection reconnect(final Connection oldConnection) {
                try {
                    Thread.sleep(500);
                }
                catch (final InterruptedException e) {
                    // Ignore.
                }
                return mockConnection2;
            }
        };

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        // Reconnect.
        mockConnection
                .raiseErrors(anyObject(MongoDbException.class), eq(false));
        expectLastCall();
        expect(myMockConnectionFactory.getReconnectStrategy()).andReturn(
                pauseStrategy);
        mockConnection
                .removePropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        // Second message.
        expect(mockConnection.isOpen()).andReturn(false).times(2);
        // Wait for the reconnect.

        // After reconnect.
        expect(mockConnection2.isOpen()).andReturn(true);
        expect(mockConnection2.getPendingCount()).andReturn(0);
        expect(mockConnection2.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                myTestInstance.handleConnectionClosed(mockConnection);
            }
        }).start();
        Thread.sleep(100);

        myTestInstance.send(message, null);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link ClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     * @throws InterruptedException
     *             On a failure to pause in the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageWaitsForReconnectTimesOut() throws IOException,
            InterruptedException {

        myConfig.setReconnectTimeout(250);

        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(1);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        makeThreadSafe(mockConnection, mockConnection2);

        final ReconnectStrategy pauseStrategy = new SimpleReconnectStrategy() {
            @Override
            public Connection reconnect(final Connection oldConnection) {
                try {
                    Thread.sleep(500);
                }
                catch (final InterruptedException e) {
                    // Ignore.
                }
                return mockConnection2;
            }
        };

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection.send(message, null)).andReturn(
                ServerNameUtils.normalize(ourServer.getInetSocketAddress()));

        // Reconnect.
        mockConnection
                .raiseErrors(anyObject(MongoDbException.class), eq(false));
        expectLastCall();
        expect(myMockConnectionFactory.getReconnectStrategy()).andReturn(
                pauseStrategy);

        // Second message.
        expect(mockConnection.isOpen()).andReturn(false).times(2);
        // Wait for the reconnect.

        // After reconnect.
        expect(mockConnection.isOpen()).andReturn(false).times(2);

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);

        new Thread(new Runnable() {
            @Override
            public void run() {
                myTestInstance.handleConnectionClosed(mockConnection);
            }
        }).start();
        Thread.sleep(100);

        try {
            myTestInstance.send(message, null);
        }
        catch (final CannotConnectException failure) {
            assertThat(
                    failure.getMessage(),
                    containsString("Could not create a connection to the server."));

        }

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
     * Performs a {@link EasyMock#makeThreadSafe(Object, boolean)} on the
     * provided mocks and the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void makeThreadSafe(final Object... mocks) {
        for (final Object mock : mocks) {
            EasyMock.makeThreadSafe(mock, true);
        }
        EasyMock.makeThreadSafe(myMockConnectionFactory, true);
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
     * Performs a {@link EasyMock#reset(Object...)} on the provided mocks and
     * the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void reset(final Object... mocks) {
        EasyMock.reset(mocks);
        EasyMock.reset(myMockConnectionFactory);
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
