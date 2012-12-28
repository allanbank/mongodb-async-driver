/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * SimpleReconnectStrategyTest provides tests for the
 * {@link SimpleReconnectStrategy} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategyTest {

    /** The address for the test. */
    private String myAddress = null;

    /**
     * Creates the basic test objects.
     */
    @Before
    public void setUp() {
        myAddress = "localhost:27017";
    }

    /**
     * Cleans up the test.
     */
    @After
    public void tearDown() {
        myAddress = null;
    }

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     * 
     * @throws IOException
     *             On a Failure setting up the mock configuration for the test.
     * @throws InterruptedException
     *             On a Failure setting up the mock configuration for the test.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReconnect() throws IOException, InterruptedException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ServerState server = new ServerState("localhost:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockSelector.pickServers()).andReturn(
                Collections.singletonList(server));

        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);

        final Capture<Callback<Reply>> callbackCapture = new Capture<Callback<Reply>>() {
            /** Serialization version for the class. */
            private static final long serialVersionUID = -8744386051520804331L;

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);

                value.callback(null);
            }
        };
        expect(
                mockNewConnection.send(anyObject(ServerStatus.class),
                        capture(callbackCapture))).andReturn(myAddress);

        mockOldConnection.drainPending((List<PendingMessage>) anyObject());
        mockNewConnection.addPending((List<PendingMessage>) anyObject());

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);

        assertSame(mockNewConnection, strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     */
    @Test
    public void testReconnectFails() {
        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockSelector.pickServers()).andReturn(
                new ArrayList<ServerState>());

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(new MongoClientConfiguration());
        strategy.setSelector(mockSelector);

        assertNull(strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     * 
     * @throws IOException
     *             On a Failure setting up the mock configuration for the test.
     * @throws InterruptedException
     *             On a Failure setting up the mock configuration for the test.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReconnectFirstFails() throws IOException,
            InterruptedException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ClusterState clusterState = new ClusterState(
                new MongoClientConfiguration());
        final ServerState server = clusterState.add("localhost:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockSelector.pickServers()).andReturn(
                Arrays.asList(server, server));

        expect(mockFactory.connect(server, config)).andThrow(new IOException());
        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);

        final Capture<Callback<Reply>> callbackCapture = new Capture<Callback<Reply>>() {
            /** Serialization version for the class. */
            private static final long serialVersionUID = -8744386051520804331L;

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);

                value.callback(null);
            }
        };
        expect(
                mockNewConnection.send(anyObject(ServerStatus.class),
                        capture(callbackCapture))).andReturn(myAddress);

        mockOldConnection.drainPending((List<PendingMessage>) anyObject());
        mockNewConnection.addPending((List<PendingMessage>) anyObject());

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(clusterState);

        assertSame(clusterState, strategy.getState());
        assertSame(mockSelector, strategy.getSelector());
        assertSame(config, strategy.getConfig());
        assertSame(mockFactory, strategy.getConnectionFactory());

        assertSame(mockNewConnection, strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     * 
     * @throws IOException
     *             On a Failure setting up the mock configuration for the test.
     */
    @Test
    public void testReconnectPingFails() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ServerState server = new ServerState("localhost:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockSelector.pickServers()).andReturn(
                Collections.singletonList(server));

        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);

        final Capture<Callback<Reply>> callbackCapture = new Capture<Callback<Reply>>() {
            /** Serialization version for the class. */
            private static final long serialVersionUID = -8744386051520804331L;

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);

                value.exception(new MongoDbException("This is a test."));
            }
        };
        expect(
                mockNewConnection.send(anyObject(ServerStatus.class),
                        capture(callbackCapture))).andReturn(myAddress);

        mockNewConnection.close();

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);

        assertNull(strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }
}
