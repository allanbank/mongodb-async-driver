/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.rs;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.MockMongoDBServer;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ReplicaSetReconnectStrategyTest provides tests for the
 * {@link ReplicaSetReconnectStrategy}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetReconnectStrategyTest {

    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer1;

    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer2;

    /** A Mock MongoDB server to connect to. */
    private static MockMongoDBServer ourServer3;

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

    /** Update document to mark servers as the secondary. */
    private static final Document SECONDARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", false)
                    .add("secondary", true));

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        ourServer1 = new MockMongoDBServer();
        ourServer1.start();
        ourServer2 = new MockMongoDBServer();
        ourServer2.start();
        ourServer3 = new MockMongoDBServer();
        ourServer3.start();
    }

    /**
     * Stops a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to stop the Mock MongoDB server.
     */
    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        ourServer1.setRunning(false);
        ourServer1.close();
        ourServer1 = null;
        ourServer2.setRunning(false);
        ourServer2.close();
        ourServer2 = null;
        ourServer3.setRunning(false);
        ourServer3.close();
        ourServer3 = null;

        // Make sure all of the driver's threads have shutdown.
        long now = System.currentTimeMillis();
        final long deadline = now + TimeUnit.SECONDS.toMillis(10);
        while (ReplicaSetConnectionFactoryTest.driverThreadRunning()
                && (now < deadline)) {
            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                // Ignored.
            }
            now = System.currentTimeMillis();

        }
    }

    /** The test connection. */
    private ReplicaSetConnection myNewTestConnection;

    /** The test connection. */
    private ReplicaSetConnection myTestConnection;

    /** The factory being tested. */
    private ReplicaSetConnectionFactory myTestFactory;

    /**
     * Cleans up the test connection.
     * 
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @After
    public void tearDown() throws IOException {
        ourServer1.clear();
        ourServer2.clear();
        ourServer3.clear();

        IOUtils.close(myTestConnection);
        IOUtils.close(myNewTestConnection);
        IOUtils.close(myTestFactory);

        myTestConnection = null;
        myNewTestConnection = null;
        myTestFactory = null;
    }

    /**
     * Test method for {@link ReplicaSetReconnectStrategy#reconnect(Connection)}
     * .
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testReconnect() throws IOException {
        final String serverName1 = ourServer1.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer1.getInetSocketAddress().getPort();
        final String serverName2 = ourServer2.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer2.getInetSocketAddress().getPort();
        final String serverName3 = ourServer3.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer3.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName1);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Servers are called twice. Once for ping and once for close.
        ourServer1.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer1.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        List<Server> servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer1.getInetSocketAddress()));

        // Bootstrapped! Yay.
        // Setup for server2 to take over.

        ourServer1.clear();
        ourServer2.clear();
        ourServer3.clear();

        replStatusBuilder.reset();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName2);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Note sure who will get asked first... server2 should be asked twice.
        ourServer1
                .setReplies(reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));
        ourServer3
                .setReplies(reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        myTestConnection = (ReplicaSetConnection) myTestFactory.connect();
        final ReplicaSetReconnectStrategy strategy = (ReplicaSetReconnectStrategy) myTestFactory
                .getReconnectStrategy();

        myNewTestConnection = strategy.reconnect(myTestConnection);

        servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer2.getInetSocketAddress()));
    }

    /**
     * Test method for {@link ReplicaSetReconnectStrategy#reconnect(Connection)}
     * .
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testReconnectDisagree() throws IOException {
        final String serverName1 = ourServer1.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer1.getInetSocketAddress().getPort();
        final String serverName2 = ourServer2.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer2.getInetSocketAddress().getPort();
        final String serverName3 = ourServer3.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer3.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName1);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Servers are called twice. Once for ping and once for close.
        ourServer1.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer1.getInetSocketAddress());
        config.setReconnectTimeout(1000);
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        List<Server> servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer1.getInetSocketAddress()));

        // Bootstrapped! Yay.
        // Setup for no one to be the new primary.

        ourServer1.clear();
        ourServer2.clear();
        ourServer3.clear();

        //
        // Create the connection.
        //

        // Should only contact the primary.
        ourServer1.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)));

        myTestConnection = (ReplicaSetConnection) myTestFactory.connect();
        final ReplicaSetReconnectStrategy strategy = (ReplicaSetReconnectStrategy) myTestFactory
                .getReconnectStrategy();

        //
        // Force a reconnect.
        //

        replStatusBuilder.reset();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName2);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        final DocumentBuilder replyUnknown = BuilderFactory.start();
        replyUnknown.push("repl");
        replyUnknown.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        final DocumentBuilder reply2 = BuilderFactory.start();
        reply2.push("repl");
        reply2.addString("primary", serverName3);
        reply2.pushArray("hosts").addString(serverName1).addString(serverName2)
                .addString(serverName3);

        // Note sure who will get asked first...
        ourServer1.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(reply(start(SECONDARY_UPDATE, replyUnknown)),
                reply(start(SECONDARY_UPDATE, replyUnknown)),
                reply(start(SECONDARY_UPDATE, replyUnknown)),
                reply(start(SECONDARY_UPDATE, replyUnknown)),
                reply(start(SECONDARY_UPDATE, replyUnknown)),
                reply(start(SECONDARY_UPDATE, replyUnknown)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        myNewTestConnection = strategy.reconnect(myTestConnection);

        assertThat(myNewTestConnection, nullValue(Connection.class));
        servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(0, servers.size());
    }

    /**
     * Test method for {@link ReplicaSetReconnectStrategy#reconnect(Connection)}
     * . This scenario have the reconnect contact a different server before
     * settling on the primary.
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testReconnectPause() throws IOException {
        final String serverName1 = ourServer1.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer1.getInetSocketAddress().getPort();
        final String serverName2 = ourServer2.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer2.getInetSocketAddress().getPort();
        final String serverName3 = ourServer3.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer3.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = BuilderFactory.start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName1);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Servers are called twice. Once for ping and once for close.
        ourServer1.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer1.getInetSocketAddress());
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        List<Server> servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer1.getInetSocketAddress()));

        // Bootstrapped! Yay.
        // Setup for server3 to take over.
        ourServer1.clear();
        ourServer2.clear();
        ourServer3.clear();

        //
        // Create the connection.
        //

        // Should only contact the primary.
        ourServer1.setReplies(reply(replStatusBuilder));

        myTestConnection = (ReplicaSetConnection) myTestFactory.connect();
        final ReplicaSetReconnectStrategy strategy = (ReplicaSetReconnectStrategy) myTestFactory
                .getReconnectStrategy();

        //
        // Force a reconnect.
        //

        // No one knows reply
        replStatusBuilder.reset();
        replStatusBuilder.push("repl");
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Positive reply - server 3.
        final DocumentBuilder reply2 = BuilderFactory.start();
        reply2.push("repl");
        reply2.addString("primary", serverName3);
        reply2.pushArray("hosts").addString(serverName1).addString(serverName2)
                .addString(serverName3);

        ourServer1.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, reply2)));
        ourServer2.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, reply2)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, reply2)),
                reply(start(PRIMARY_UPDATE, reply2)));

        myNewTestConnection = strategy.reconnect(myTestConnection);

        servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer3.getInetSocketAddress()));
    }

    /**
     * Test method for {@link ReplicaSetReconnectStrategy#reconnect(Connection)}
     * .
     * 
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testReconnectTimeout() throws IOException {
        final String serverName1 = ourServer1.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer1.getInetSocketAddress().getPort();
        final String serverName2 = ourServer2.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer2.getInetSocketAddress().getPort();
        final String serverName3 = ourServer3.getInetSocketAddress()
                .getHostName()
                + ":"
                + ourServer3.getInetSocketAddress().getPort();

        final DocumentBuilder replStatusBuilder = start();
        replStatusBuilder.push("repl");
        replStatusBuilder.addString("primary", serverName1);
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        // Servers are called twice. Once for ping and once for close.
        ourServer1.setReplies(reply(start(PRIMARY_UPDATE, replStatusBuilder)),
                reply(start(PRIMARY_UPDATE, replStatusBuilder)));
        ourServer2.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));
        ourServer3.setReplies(
                reply(start(SECONDARY_UPDATE, replStatusBuilder)),
                reply(start(SECONDARY_UPDATE, replStatusBuilder)));

        final MongoClientConfiguration config = new MongoClientConfiguration(
                ourServer1.getInetSocketAddress());
        config.setReconnectTimeout(1000);
        config.setAutoDiscoverServers(true);

        final ProxiedConnectionFactory socketFactory = new SocketConnectionFactory(
                config);
        myTestFactory = new ReplicaSetConnectionFactory(socketFactory, config);

        List<Server> servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(1, servers.size());
        assertThat(servers.get(0).getAddresses(),
                hasItem(ourServer1.getInetSocketAddress()));

        // Bootstrapped! Yay.
        // Setup for server3 to take over.
        ourServer1.clear();
        ourServer2.clear();
        ourServer3.clear();

        //
        // Create the connection.
        //

        // Should only contact the primary.
        ourServer1.setReplies(reply(replStatusBuilder));

        myTestConnection = (ReplicaSetConnection) myTestFactory.connect();
        final ReplicaSetReconnectStrategy strategy = (ReplicaSetReconnectStrategy) myTestFactory
                .getReconnectStrategy();

        //
        // Force a reconnect.
        //

        // No one knows reply
        replStatusBuilder.reset();
        replStatusBuilder.push("repl");
        replStatusBuilder.pushArray("hosts").addString(serverName1)
                .addString(serverName2).addString(serverName3);

        ourServer1.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));
        ourServer2.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));
        ourServer3.setReplies(reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder),
                reply(replStatusBuilder), reply(replStatusBuilder));

        myNewTestConnection = strategy.reconnect(myTestConnection);
        assertNull(myNewTestConnection);

        servers = myTestFactory.getCluster().getWritableServers();
        assertEquals(0, servers.size());
    }
}
