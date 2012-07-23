/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.bootstrap;

import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.connection.auth.AuthenticationConnectionFactory;
import com.allanbank.mongodb.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;

/**
 * Integration test for the {@link BootstrapConnectionFactory}.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactoryITest extends ServerTestDriverSupport {

    /**
     * Stops all of the background processes started.
     */
    @After
    public void tearDown() {
        stopReplicaSet();
        stopSharded();
        stopStandAlone();
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapReplicaSet() {
        startReplicaSet();

        final MongoDbConfiguration config = new MongoDbConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                config);

        assertThat("Wrong type of factory.", factory.getDelegate(),
                CoreMatchers.instanceOf(ReplicaSetConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapSharded() {
        startSharded();

        final MongoDbConfiguration config = new MongoDbConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                config);

        assertThat("Wrong type of factory.", factory.getDelegate(),
                CoreMatchers.instanceOf(ShardedConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandalone() {
        startStandAlone();

        final MongoDbConfiguration config = new MongoDbConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                config);

        assertThat("Wrong type of factory.", factory.getDelegate(),
                CoreMatchers.instanceOf(SocketConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandaloneWithAuth() {
        startAuthenticated();

        final MongoDbConfiguration config = new MongoDbConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        config.authenticate(USER_NAME, PASSWORD);

        final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                config);

        assertThat("Wrong type of factory.", factory.getDelegate(),
                CoreMatchers.instanceOf(AuthenticationConnectionFactory.class));
    }
}
