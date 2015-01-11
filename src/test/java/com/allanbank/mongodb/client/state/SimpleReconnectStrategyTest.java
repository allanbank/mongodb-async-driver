/*
 * #%L
 * SimpleReconnectStrategyTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.client.state;

import static com.allanbank.mongodb.client.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.ServerStatus;

/**
 * SimpleReconnectStrategyTest provides tests for the
 * {@link SimpleReconnectStrategy} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleReconnectStrategyTest {

    /** Update document to mark servers as the primary. */
    private static final Document PRIMARY_UPDATE = new ImmutableDocument(
            BuilderFactory.start().add("ismaster", true));

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     *
     * @throws IOException
     *             On a Failure setting up the mock configuration for the test.
     * @throws InterruptedException
     *             On a Failure setting up the mock configuration for the test.
     */
    @Test
    public void testReconnect() throws IOException, InterruptedException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("foo:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockOldConnection.getServerName()).andReturn("foo:27017");
        expect(mockFactory.connect(server, config)).andThrow(
                new IOException("Inject"));

        expect(mockSelector.pickServers()).andReturn(
                Collections.singletonList(server));

        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);

        mockNewConnection.send(anyObject(IsMaster.class),
                cb(BuilderFactory.start(PRIMARY_UPDATE)));
        expectLastCall();

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(cluster);

        assertSame(mockNewConnection, strategy.reconnect(mockOldConnection));

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
    @Test
    public void testReconnectBackWorks() throws IOException,
            InterruptedException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("foo:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockOldConnection.getServerName()).andReturn("foo:27017");
        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);
        mockNewConnection.send(anyObject(IsMaster.class),
                cb(BuilderFactory.start(PRIMARY_UPDATE)));
        expectLastCall();

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(cluster);

        assertSame(mockNewConnection, strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }

    /**
     * Test method for {@link SimpleReconnectStrategy#reconnect(Connection)}.
     *
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testReconnectFails() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("foo:27017");

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockOldConnection.getServerName()).andReturn("foo:27017");
        expect(mockFactory.connect(server, config)).andThrow(
                new IOException("Inject"));

        expect(mockSelector.pickServers()).andReturn(new ArrayList<Server>());

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(cluster);

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
    @Test
    public void testReconnectFirstFails() throws IOException,
            InterruptedException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add(new InetSocketAddress("foo", 27017));

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockOldConnection.getServerName()).andReturn("foo:27017");

        expect(mockFactory.connect(server, config)).andThrow(
                new IOException("Inject"));

        expect(mockSelector.pickServers()).andReturn(
                Arrays.asList(server, server));

        expect(mockFactory.connect(server, config)).andThrow(new IOException());
        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);

        mockNewConnection.send(anyObject(ServerStatus.class),
                cb(BuilderFactory.start(PRIMARY_UPDATE)));
        expectLastCall();

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(cluster);

        assertSame(cluster, strategy.getState());
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
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add(new InetSocketAddress("foo", 27017));

        final Connection mockOldConnection = createMock(Connection.class);
        final Connection mockNewConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);

        expect(mockOldConnection.getServerName()).andReturn("foo:27017");
        expect(mockFactory.connect(server, config)).andThrow(
                new IOException("Inject"));

        expect(mockSelector.pickServers()).andReturn(
                Collections.singletonList(server));
        expect(mockFactory.connect(server, config))
                .andReturn(mockNewConnection);
        mockNewConnection.send(anyObject(ServerStatus.class),
                cb(new MongoDbException("Injected")));
        expectLastCall();

        mockNewConnection.close();

        replay(mockOldConnection, mockNewConnection, mockFactory, mockSelector);

        final SimpleReconnectStrategy strategy = new SimpleReconnectStrategy();

        strategy.setConnectionFactory(mockFactory);
        strategy.setConfig(config);
        strategy.setSelector(mockSelector);
        strategy.setState(cluster);

        assertNull(strategy.reconnect(mockOldConnection));

        verify(mockOldConnection, mockNewConnection, mockFactory, mockSelector);
    }
}
