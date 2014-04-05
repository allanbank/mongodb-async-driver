/*
 * Copyright 2011, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.bootstrap;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.ConnectionFactory;
import com.allanbank.mongodb.client.connection.MockMongoDBServer;
import com.allanbank.mongodb.client.connection.auth.AuthenticationConnectionFactory;
import com.allanbank.mongodb.client.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.client.connection.rs.ReplicaSetReconnectStrategy;
import com.allanbank.mongodb.client.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.SimpleReconnectStrategy;
import com.allanbank.mongodb.error.CannotConnectException;

/**
 * Integration test for the {@link BootstrapConnectionFactory}.
 *
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactoryTest {

    /** A test password. You really shouldn't use this as a password... */
    public static final char[] PASSWORD = "password".toCharArray();

    /** A test user name. */
    public static final String USER_NAME = "user";

    /** Update document with the "build info". */
    private static final Document BUILD_INFO = new ImmutableDocument(
            BuilderFactory.start().add(Server.MAX_BSON_OBJECT_SIZE_PROP,
                    Client.MAX_DOCUMENT_SIZE));

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
    private BootstrapConnectionFactory myTestFactory;

    /**
     * Cleans up the test connection.
     *
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @After
    public void tearDown() throws IOException {
        myTestFactory.close();
        myTestFactory = null;
        ourServer.clear();
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapAllFail() {
        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod");

        ourServer.setReplies(reply(start(BUILD_INFO)), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        try {
            myTestFactory.getDelegate();
            fail("Should have thrown a CannotConnectException.");
        }
        catch (final CannotConnectException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     *
     * @throws UnknownHostException
     *             On a failure to resolve localhost.
     */
    @Test
    public void testBootstrapInterrupted() throws UnknownHostException {
        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod");

        ourServer
                .setReplies(reply(start(BUILD_INFO)), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());

        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        Thread.currentThread().interrupt();
        try {
            myTestFactory.getDelegate();
            fail("Should have failed.");
        }
        catch (final CannotConnectException error) {
            // Good
        }

        // Try again and it should work (not interrupted any more).
        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                instanceOf(SocketConnectionFactory.class));
        assertThat(myTestFactory.getReconnectStrategy(),
                instanceOf(SimpleReconnectStrategy.class));
        assertEquals(ClusterType.STAND_ALONE, myTestFactory.getClusterType());
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     *
     * @throws UnknownHostException
     *             On a failure to resolve localhost.
     */
    @Test
    public void testBootstrapNotOkReply() throws UnknownHostException {
        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod").add("ok", 0);

        ourServer
                .setReplies(reply(start(BUILD_INFO)), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());

        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        Thread.currentThread().interrupt();
        try {
            myTestFactory.getDelegate();
            fail("Should have failed.");
        }
        catch (final CannotConnectException error) {
            // Good
        }
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapReplicaSet() {

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("setName", "foo");

        ourServer.setReplies(reply(start(BUILD_INFO)),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                instanceOf(ReplicaSetConnectionFactory.class));
        assertThat(myTestFactory.getReconnectStrategy(),
                instanceOf(ReplicaSetReconnectStrategy.class));
        assertEquals(ClusterType.REPLICA_SET, myTestFactory.getClusterType());
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapSharded() {

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("msg", "isdbgrid");

        ourServer.setReplies(reply(start(BUILD_INFO)),
                reply(replStatusBuilder), reply());

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                instanceOf(ShardedConnectionFactory.class));
        assertThat(myTestFactory.getReconnectStrategy(),
                instanceOf(SimpleReconnectStrategy.class));
        assertEquals(ClusterType.SHARDED, myTestFactory.getClusterType());
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandalone() {
        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod");

        ourServer
                .setReplies(reply(start(BUILD_INFO)), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                instanceOf(SocketConnectionFactory.class));
        assertThat(myTestFactory.getReconnectStrategy(),
                instanceOf(SimpleReconnectStrategy.class));
        assertEquals(ClusterType.STAND_ALONE, myTestFactory.getClusterType());
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandaloneFirstFails() {
        final InetSocketAddress fails = new InetSocketAddress(ourServer
                .getInetSocketAddress().getAddress(), ourServer
                .getInetSocketAddress().getPort() + 1);

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod");

        ourServer
                .setReplies(reply(start(BUILD_INFO)), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                fails, ourServer.getInetSocketAddress());
        config.setAutoDiscoverServers(false);

        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(SocketConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandaloneWithAuth() {
        final DocumentBuilder nonceReply = BuilderFactory.start();
        nonceReply.addString("nonce", "deadbeef4bee");

        final DocumentBuilder authReply = BuilderFactory.start();
        authReply.addInteger("ok", 1);

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.addString("process", "mongod");

        ourServer.setReplies(reply(start(BUILD_INFO)), reply(nonceReply),
                reply(authReply), reply(replStatusBuilder));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer.getInetSocketAddress());
        config.addCredential(Credential.builder().userName(USER_NAME)
                .password(PASSWORD).database("foo")
                .authenticationType(Credential.MONGODB_CR).build());

        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(AuthenticationConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#close()} .
     *
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        myTestFactory = new BootstrapConnectionFactory(config);

        final ConnectionFactory mockFactory = createMock(ConnectionFactory.class);

        mockFactory.close();
        expectLastCall();

        replay(mockFactory);

        myTestFactory.setDelegate(mockFactory);
        myTestFactory.close();

        verify(mockFactory);

        // Reset the mock for a close in tearDown.
        reset(mockFactory);
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#connect()} .
     */
    @Test
    public void testConnect() {
        try {
            final MongoClientConfiguration config = new MongoClientConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            myTestFactory = new BootstrapConnectionFactory(config);

            final Connection mockConnection = createMock(Connection.class);
            final ConnectionFactory mockFactory = createMock(ConnectionFactory.class);

            expect(mockFactory.connect()).andReturn(mockConnection);

            replay(mockConnection, mockFactory);

            myTestFactory.setDelegate(mockFactory);
            assertSame(mockConnection, myTestFactory.connect());

            verify(mockConnection, mockFactory);

            // Reset the mock for a close in tearDown.
            reset(mockFactory);
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
    }

}
