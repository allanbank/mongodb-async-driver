/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
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
import static org.hamcrest.Matchers.describedAs;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.MockMongoDBServer;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.ReplicaSetStatus;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.Server;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * ReplicaSetConnectionFactoryTest provides tests for the
 * {@link ReplicaSetConnectionFactory}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnectionFactoryTest {
    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer;

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

    /**
     * Returns true if a driver thread is found.
     * 
     * @return True if a driver thread is found.
     */
    public static final boolean driverThreadRunning() {
        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @BeforeClass
    public static void setUpServer() throws IOException {
        ourServer = new MockMongoDBServer();
        ourServer.start();
    }

    /**
     * Makes sure all of the threads have terminates at the end of the tests.
     * 
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @AfterClass
    public static void tearDownClass() throws IOException {
        ourServer.setRunning(false);
        ourServer.close();
        ourServer = null;

        // Make sure all of the driver's threads have shutdown.
        long now = System.currentTimeMillis();
        final long deadline = now + TimeUnit.SECONDS.toMillis(10);
        while (driverThreadRunning() && (now < deadline)) {
            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                // Ignored.
            }
            now = System.currentTimeMillis();

        }

        assertThat(driverThreadRunning(),
                describedAs("Found a driver thread.", is(false)));
    }

    /** The factory being tested. */
    private ReplicaSetConnectionFactory myTestFactory;

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @Before
    public void setUp() throws IOException {
        myTestFactory = null;

        if (!ourServer.isRunning()) {
            ourServer = new MockMongoDBServer();
            ourServer.start();
        }
    }

    /**
     * Cleans up the test connection and stops a Mock MongoDB server.
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
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrap() {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final String serverName = ServerNameUtils.normalize(addr);

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

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapAddPrimary() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
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

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(2, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoDiscover() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
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

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(serverName);
        config.setAutoDiscoverServers(false);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(1, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoPrimary() {
        final String serverName = ourServer.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://" + serverName);
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(1, servers.size());
        assertFalse(servers.get(0).isWritable());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#bootstrap()}.
     */
    @Test
    public void testBootstrapNoReplyDocs() {

        ourServer.setReplies(reply(), reply(), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(1, servers.size());
        assertFalse(servers.get(0).isWritable());
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#close()} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final String serverName = ServerNameUtils.normalize(addr);

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234");

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        final List<Server> servers = myTestFactory.getCluster().getServers();
        assertEquals(2, servers.size());

        myTestFactory.close();
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnect() throws IOException {
        final String serverName = ServerNameUtils.normalize(ourServer
                .getInetSocketAddress());

        final DocumentBuilder replStatusBuilder = start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        ourServer.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));

        final MongoClientConfiguration config = new MongoClientConfiguration(
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
     * @throws InterruptedException
     *             On a failure.
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

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        ourServer.setRunning(false);
        ourServer.close();
        Thread.sleep(100); // Make sure the socket is not connectable.
        try {
            myTestFactory = new ReplicaSetConnectionFactory(socketFactory,
                    config);

            final Connection connection = myTestFactory.connect();
            IOUtils.close(connection);
            fail("Should have failed to connect.");
        }
        catch (final IOException ioe) {
            // Good.
        }
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     */
    @Test
    public void testConnectNoServer() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

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
     * @throws InterruptedException
     *             On a failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConnectReplyWeird() throws IOException,
            InterruptedException {
        final String serverName = "foo:27017";

        final DocumentBuilder replStatusBuilder = BuilderFactory
                .start(PRIMARY_UPDATE);
        replStatusBuilder.add("me", serverName);
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(serverName);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        // The request to find the cluster.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Now the ping sweep.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        expect(
                mockConnection.send(eq(new ReplicaSetStatus()),
                        cb(replStatusBuilder))).andReturn(serverName);
        mockConnection.shutdown();
        expectLastCall();

        //
        // End of the constructor... Now for the connect.
        //

        // Any empty reply.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb())).andReturn(
                serverName);
        mockConnection.close();
        expectLastCall();

        // Locate the primary...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // An Interrupted thread...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andAnswer(new IAnswer<Connection>() {
                    @Override
                    public Connection answer() throws Throwable {
                        // For the next check....
                        Thread.currentThread().interrupt();
                        return mockConnection;
                    }
                });
        expect(
                mockConnection.send(eq(new IsMaster()),
                        anyObject(Callback.class))).andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Locate the primary...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Execution error.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(
                mockConnection.send(eq(new IsMaster()), cb(new IOException(
                        "Injected.")))).andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Locate the primary...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // No primary field in reply.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(
                mockConnection.send(eq(new IsMaster()),
                        cb(start(PRIMARY_UPDATE)))).andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Locate the primary...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Finally success...
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        expect(mockConnection.getServerName()).andReturn(serverName);

        // A clean close.
        mockConnection.close();
        expectLastCall();
        mockFactory.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        try {
            myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);

            final Connection connection = myTestFactory.connect();
            IOUtils.close(connection);
        }
        finally {
            IOUtils.close(myTestFactory);
            myTestFactory = null;
        }

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsExecutionError() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.shutdown();
        expectLastCall();

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
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
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
     * @throws InterruptedException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsIOException() throws IOException,
            InterruptedException {
        final String serverName = "foo:27017";

        final DocumentBuilder replStatusBuilder = BuilderFactory
                .start(PRIMARY_UPDATE);
        replStatusBuilder.add("me", serverName);
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(serverName);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        // The request to find the cluster.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Now the ping sweep.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        expect(
                mockConnection.send(eq(new ReplicaSetStatus()),
                        cb(replStatusBuilder))).andReturn(serverName);
        mockConnection.shutdown();
        expectLastCall();

        //
        // End of the constructor... Now for the connect.
        //

        final IOException thrown = new IOException("Injected");
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andThrow(thrown).times(11);

        // A clean close.
        mockFactory.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        try {
            myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);

            final Connection connection = myTestFactory.connect();
            IOUtils.close(connection);
            fail("Should have failed to connect.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe, sameInstance(thrown));
        }
        finally {
            IOUtils.close(myTestFactory);
            myTestFactory = null;
        }

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testConnectThrowsMongoError() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer("localhost:6547");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection).times(2);

        mockConnection.send(anyObject(IsMaster.class), cb());
        expectLastCall().andThrow(new MongoDbException("This is a test"))
                .times(2);

        mockConnection.shutdown();
        expectLastCall();

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
     * @throws InterruptedException
     *             On a failure.
     */
    @Test
    public void testExceptionsInBootstrap() throws IOException,
            InterruptedException {
        final String serverName = "foo:27017";

        final DocumentBuilder replStatusBuilder = BuilderFactory
                .start(PRIMARY_UPDATE);
        replStatusBuilder.add("me", serverName);
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(serverName);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        // The request to find the cluster.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(
                mockConnection.send(eq(new IsMaster()), cb(new IOException(
                        "Injected.")))).andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Now the ping sweep.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        expect(
                mockConnection.send(eq(new ReplicaSetStatus()),
                        cb(replStatusBuilder))).andReturn(serverName);
        mockConnection.shutdown();
        expectLastCall();

        //
        // End of the constructor... Now for the connect.
        //

        // A clean close.
        mockFactory.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        try {
            Thread.currentThread().interrupt();
            myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);
        }
        finally {
            IOUtils.close(myTestFactory);
            myTestFactory = null;
        }

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link ReplicaSetConnectionFactory#getClusterType()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testGetClusterType() throws IOException {
        final String serverName = "localhost:"
                + ourServer.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName)
                .addString("localhost:1234");

        ourServer.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);

        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        assertEquals(ClusterType.REPLICA_SET, myTestFactory.getClusterType());
    }

    /**
     * Test method for
     * {@link ReplicaSetConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {

        final MongoClientConfiguration config = new MongoClientConfiguration();
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

    /**
     * Test method for {@link ReplicaSetConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure.
     * @throws InterruptedException
     *             On a failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInterruptedBootstrap() throws IOException,
            InterruptedException {
        final String serverName = "foo:27017";

        final DocumentBuilder replStatusBuilder = BuilderFactory
                .start(PRIMARY_UPDATE);
        replStatusBuilder.add("me", serverName);
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName);
        replStatusBuilder.pushArray("hosts").addString(serverName);

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(serverName);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        // The request to find the cluster.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(
                mockConnection.send(eq(new IsMaster()),
                        anyObject(Callback.class))).andReturn(serverName);
        mockConnection.close();
        expectLastCall();

        // Now the ping sweep.
        expect(mockFactory.connect(anyObject(Server.class), eq(config)))
                .andReturn(mockConnection);
        expect(mockConnection.send(eq(new IsMaster()), cb(replStatusBuilder)))
                .andReturn(serverName);
        expect(
                mockConnection.send(eq(new ReplicaSetStatus()),
                        cb(replStatusBuilder))).andReturn(serverName);
        mockConnection.shutdown();
        expectLastCall();

        //
        // End of the constructor... Now for the connect.
        //

        // A clean close.
        mockFactory.close();
        expectLastCall();

        replay(mockFactory, mockConnection);

        try {
            Thread.currentThread().interrupt();
            myTestFactory = new ReplicaSetConnectionFactory(mockFactory, config);
        }
        finally {
            IOUtils.close(myTestFactory);
            myTestFactory = null;
        }

        verify(mockFactory, mockConnection);
    }
}
