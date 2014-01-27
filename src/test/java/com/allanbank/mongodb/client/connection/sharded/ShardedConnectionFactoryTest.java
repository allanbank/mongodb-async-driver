/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.sharded;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
import static com.allanbank.mongodb.client.connection.CallbackReply.cb;
import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.MockMongoDBServer;
import com.allanbank.mongodb.client.connection.ReconnectStrategy;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.SimpleReconnectStrategy;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * ShardedConnectionFactoryTest provides tests for the
 * {@link ShardedConnectionFactory}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactoryTest {

    /** Update document with the "build info". */
    private static final Document BUILD_INFO = new ImmutableDocument(
            BuilderFactory.start().add(Server.MAX_BSON_OBJECT_SIZE_PROP,
                    Client.MAX_DOCUMENT_SIZE));

    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer;

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

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
     */
    @After
    public void tearDown() {
        IOUtils.close(myTestFactory);
        myTestFactory = null;
        ourServer.clear();
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrap() {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final String serverName = ServerNameUtils.normalize(addr);

        ourServer.setReplies(
                reply(start(BUILD_INFO)),
                reply(start().addString("_id", serverName), BuilderFactory
                        .start().addString("_id", "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoDiscover() {
        ourServer.setReplies(reply(start(BUILD_INFO)), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(1, servers.size());

        assertEquals(3, ourServer.getRequests().size()); // For buildInfo + ping
                                                         // + request.
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBootstrapThrowsExecutionError() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        // Query for servers.
        mockConnection.send(anyObject(Query.class), cb(new IOException(
                "Injected.")));
        expectLastCall();

        // Ping.
        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall();

        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBootstrapThrowsInterruptedException() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        // Query for servers.
        mockConnection.send(anyObject(Query.class), cb());
        expectLastCall().andAnswer(new IAnswer<String>() {
            @Override
            public String answer() throws Throwable {
                Thread.currentThread().interrupt();
                return null;
            }
        });

        // Ping.
        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall();

        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBootstrapThrowsIOError() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andThrow(new IOException("This is a test")).times(2);

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBootstrapThrowsMongoDbExecption() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#bootstrap()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testBootstrapThrowsMongoError() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#close()} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final String serverName = ServerNameUtils.normalize(addr);

        ourServer.setReplies(
                reply(start(BUILD_INFO)),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final Connection mockConnection = createMock(Connection.class);

        replay(mockConnection);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(2, servers.size());

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
                .getHostName()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.add("ismaster", true);
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer.setReplies(reply(start(BUILD_INFO)),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        final Connection connection = myTestFactory.connect();
        IOUtils.close(connection);

        assertThat(connection, instanceOf(ShardedConnection.class));
        assertThat(connection.toString(), startsWith("Sharded(MongoDB("));
    }

    /**
     * Test method for {@link ShardedConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     * @throws InterruptedException
     *             On a failure to sleep in the test.
     */
    @Test
    public void testConnectFails() throws IOException, InterruptedException {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer.setReplies(reply(start(BUILD_INFO)),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        tearDownAfterClass();
        Thread.sleep(100); // Wait for close to finish.
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
        final MongoClientConfiguration config = new MongoClientConfiguration();

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
    public void testConnectOnIOException() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        // Query for servers.
        mockConnection.send(anyObject(Query.class), cb());
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        // Ping.
        mockConnection.send(anyObject(IsMaster.class),
                cb(BuilderFactory.start(PRIMARY_UPDATE)));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        // Connect
        final IOException thrown = new IOException("Injected");
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andThrow(thrown);

        replay(mockFactory, mockConnection);

        myTestFactory = new ShardedConnectionFactory(mockFactory, config);
        assertNotNull(myTestFactory);

        try {
            final Connection conn = myTestFactory.connect();
            IOUtils.close(conn);
            fail("Should havethrown an IOException.");
        }
        catch (final IOException good) {
            assertThat(good, sameInstance(thrown));
        }

        verify(mockFactory, mockConnection);

        // Reset the mock for the close() in teardown.
        reset(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ShardedConnectionFactory#getClusterType()}.
     */
    @Test
    public void testGetClusterType() {
        final String serverName = "localhost:"
                + ourServer.getInetSocketAddress().getPort();

        ourServer.setReplies(
                reply(start(BUILD_INFO)),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ShardedConnectionFactory(socketFactory, config);

        assertEquals(ClusterType.SHARDED, myTestFactory.getClusterType());
    }

    /**
     * Test method for {@link ShardedConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        ourServer.setReplies(
                reply(start(BUILD_INFO)),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")),
                reply(BuilderFactory.start().addString("_id", serverName),
                        BuilderFactory.start().addString("_id",
                                "localhost:1234")), reply(), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
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
