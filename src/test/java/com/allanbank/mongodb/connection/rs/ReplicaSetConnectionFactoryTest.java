/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
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
import com.allanbank.mongodb.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ReplicaSetConnectionFactoryTest provides tests for the
 * {@link ReplicaSetConnectionFactory}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactoryTest {

    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer;

    /** The factory being tested. */
    private ReplicaSetConnectionFactory myTestFactory;

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @Before
    public void setUpBeforeClass() throws IOException {
        ourServer = new MockMongoDBServer();
        ourServer.start();
    }

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

        // ourServer.clear();
    }

    /**
     * Stops a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to stop the Mock MongoDB server.
     */
    @After
    public void tearDownAfterClass() throws IOException {
        ourServer.setRunning(false);
        ourServer.close();
        ourServer = null;
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrap() {
        final String serverName = "localhost:"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234");

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapAddPrimary() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", "localhost:6789");
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234").addString("localhost:6789");

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoDiscover() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234");

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.addServer(serverName);
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(1, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoPrimary() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostString()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://" + serverName);
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(1, servers.size());
        assertFalse(servers.get(0).isWritable());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoReplyDocs() {

        ourServer.setReplies(reply(), reply());

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(1, servers.size());
        assertFalse(servers.get(0).isWritable());
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

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234");

        ourServer
                .setReplies(reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<ServerState> servers = myTestFactory.getClusterState()
                .getServers();
        assertEquals(2, servers.size());

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        IOUtils.close(servers.get(0).takeConnection());
        servers.get(0).addConnection(mockConnection);
        myTestFactory.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
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

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final Connection connection = myTestFactory.connect();
        IOUtils.close(connection);

        assertThat(connection, instanceOf(ReplicaSetConnection.class));
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
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

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder));

        final MongoDbConfiguration config = new MongoDbConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

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
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     */
    @Test
    public void testConnectNoServer() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        replay(mockFactory);

        myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);

        try {
            myTestFactory.connect();
        }
        catch (final IOException ioe) {
            // Good.
        }

        verify(mockFactory);

        // Reset the mock factory for a close in tearDown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
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
                .andReturn(mockConnection).times(2);

        mockConnection.send(cb(), anyObject(IsMaster.class));
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock factory for a close in tearDown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
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
                .andThrow(new IOException("This is a test")).times(2);

        replay(mockFactory, mockConnection);

        myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock factory for a close in tearDown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
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
                .andReturn(mockConnection).times(2);

        mockConnection.send(cb(), anyObject(IsMaster.class));
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock factory for a close in tearDown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for
     * {@link ReplicaSetConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {

        final MongoDbConfiguration config = new MongoDbConfiguration();
        final ProxiedConnectionFactory mockFactory = EasyMock
                .createMock(ProxiedConnectionFactory.class);

        replay(mockFactory);

        myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);

        final ReconnectStrategy strategy = myTestFactory.getReconnectStrategy();

        assertThat(strategy, instanceOf(ReplicaSetReconnectStrategy.class));

        final ReplicaSetReconnectStrategy rsStrategy = (ReplicaSetReconnectStrategy) strategy;
        assertSame(config, rsStrategy.getConfig());
        assertSame(mockFactory, rsStrategy.getConnectionFactory());

        verify(mockFactory);

        // Reset the mock factory for a close in tearDown.
        reset(mockFactory);
    }
}
