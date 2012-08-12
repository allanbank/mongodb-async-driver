/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import static com.allanbank.mongodb.connection.CallbackReply.cb;
import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.MockMongoDBServer;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.connection.state.SimpleReconnectStrategy;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ShardedConnectionFactoryTest provides tests for the
 * {@link ShardedConnectionFactory}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactoryTest {

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

    /** The factory being tested. */
    private ShardedConnectionFactory myTestFactory;

    /**
     * Cleans up the test connection.
     * 
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @After
    public void tearDown() throws IOException {
        IOUtils.close(myTestFactory);
        myTestFactory = null;
        ourServer.clear();
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrap() {
        final String serverName = "localhost:"
                + ourServer.getInetSocketAddress().getPort();

        ourServer.setReplies(
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoDiscover() {
        ourServer.setReplies();

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(1, servers.size());

        assertEquals(0, ourServer.getRequests().size());
    }

    /**
     * Test method for {@link ShardedConnectionFactory#close()} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final String serverName = "localhost:"
                + ourServer.getInetSocketAddress().getPort();

        ourServer.setReplies(
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(2, servers.size());

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        servers.get(0).addConnection(mockConnection);
        myTestFactory.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnect() throws IOException {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer
                .setReplies(reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final Connection connection = myTestFactory.connect();
        IOUtils.close(connection);

        assertThat(connection, instanceOf(ShardedConnection.class));
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectFails() throws IOException {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer
                .setReplies(reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        tearDownAfterClass();
        try {
            final Connection connection = myTestFactory.connect();
            IOUtils.close(connection);
            fail("Should have failed to connect.");
        }
        catch (final IOException ioe) {
            // Good.
        }
        finally {
            setUpBeforeClass();
        }
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     */
    @Test
    public void testConnectNoServer() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        final ProxiedConnectionFactory mockFactory = EasyMock
                .createMock(ProxiedConnectionFactory.class);

        replay(mockFactory);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);

        try {
            myTestFactory.connect();
        }
        catch (final IOException ioe) {
            // Good.
        }

        verify(mockFactory);

        // Reset the mock for the close() in teardown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsExecutionError() throws IOException {
        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(ServerState.class), eq(config)))
                .andReturn(mockConnection);

        mockConnection.send(cb(), anyObject(IsMaster.class));
        expectLastCall().andThrow(new MongoDbException("This is a test"));

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsIOError() throws IOException {
        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(ServerState.class), eq(config)))
                .andThrow(new IOException("This is a test"));

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsMongoError() throws IOException {
        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(ServerState.class), eq(config)))
                .andReturn(mockConnection);

        mockConnection.send(cb(), anyObject(IsMaster.class));
        expectLastCall().andThrow(new MongoDbException("This is a test"));

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {
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
                                "localhost:1234")));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final ReconnectStrategy strategy = myTestFactory.getReconnectStrategy();

        assertThat(strategy, instanceOf(SimpleReconnectStrategy.class));

        final SimpleReconnectStrategy rsStrategy = (SimpleReconnectStrategy) strategy;
        assertSame(config, rsStrategy.getConfig());
        assertSame(socketFactory, rsStrategy.getConnectionFactory());
    }
}
