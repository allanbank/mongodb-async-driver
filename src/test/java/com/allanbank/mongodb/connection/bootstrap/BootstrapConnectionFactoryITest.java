/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.bootstrap;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
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

    /**
     * Test method for {@link BootstrapConnectionFactory#connect()} .
     */
    @Test
    public void testConnect() {
        try {
            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                    config);

            final Connection mockConnection = createMock(Connection.class);
            final ConnectionFactory mockFactory = createMock(ConnectionFactory.class);

            expect(mockFactory.connect()).andReturn(mockConnection);

            replay(mockConnection, mockFactory);

            factory.setDelegate(mockFactory);
            assertSame(mockConnection, factory.connect());

            verify(mockConnection, mockFactory);
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
    }
}
