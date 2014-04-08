/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.sharded;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyMultipleConnection;
import com.allanbank.mongodb.client.connection.proxy.ConnectionInfo;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.state.ServerSelector;

/**
 * ShardedConnectionTest provides tests for the {@link ShardedConnection}.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionTest {

    /**
     * Test method for {@link ShardedConnection#connect(Server)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testConnectServer() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final Server server2 = cluster.add("localhost:27018");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();
        final Capture<PropertyChangeListener> listener2 = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockFactory.connect(server2, config)).andReturn(mockConnection2);
        mockConnection2.addPropertyChangeListener(capture(listener2));
        expectLastCall();

        replay(mockConnection, mockConnection2, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.connect(server2), is(mockConnection2));

        verify(mockConnection, mockConnection2, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockConnection2, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection2.removePropertyChangeListener(listener2.getValue());
        expectLastCall();
        mockConnection.close();
        expectLastCall();
        mockConnection2.close();
        expectLastCall();

        replay(mockConnection, mockConnection2, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockConnection2, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#connect(Server)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testConnectServerAlreadyConnected() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockFactory.connect(server, config)).andReturn(mockConnection2);
        mockConnection2.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockConnection2, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.connect(server), is(mockConnection));

        verify(mockConnection, mockConnection2, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockConnection2, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockConnection2, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockConnection2, mockSelector, mockFactory);
    }

    /**
     * Test method for
     * {@link ShardedConnection#findPotentialKeys(Message, Message)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testFindPotentialKeysForFirstMessage() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final Server server2 = cluster.add("localhost:27018");

        final Message msg1 = new Command("db", Find.ALL,
                ReadPreference.server("localhost:27018"));
        final Message msg2 = new Command("db", Find.ALL);

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final List<Server> servers = conn.findPotentialKeys(msg1, msg2);
        assertThat(servers.size(), is(1));
        assertThat(servers.get(0), is(server2));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for
     * {@link ShardedConnection#findPotentialKeys(Message, Message)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testFindPotentialKeysForNeitherMessage() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Message msg1 = new Command("db", Find.ALL);
        final Message msg2 = new Command("db", Find.ALL);

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final List<Server> servers = conn.findPotentialKeys(msg1, msg2);
        assertThat(servers.size(), is(1));
        assertThat(servers.get(0), is(server));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for
     * {@link ShardedConnection#findPotentialKeys(Message, Message)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testFindPotentialKeysForNullSecond() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Message msg1 = new Command("db", Find.ALL);

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final List<Server> servers = conn.findPotentialKeys(msg1, null);
        assertThat(servers.size(), is(1));
        assertThat(servers.get(0), is(server));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for
     * {@link ShardedConnection#findPotentialKeys(Message, Message)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testFindPotentialKeysForSecondMessage() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final Server server2 = cluster.add("localhost:27018");

        final Message msg1 = new Command("db", Find.ALL, ReadPreference.PRIMARY);
        final Message msg2 = new Command("db", Find.ALL,
                ReadPreference.server("localhost:27018"));

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final List<Server> servers = conn.findPotentialKeys(msg1, msg2);
        assertThat(servers.size(), is(1));
        assertThat(servers.get(0), is(server2));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#getConnectionType()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetConnectionType() throws IOException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.getConnectionType(), is("Sharded"));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#getPendingCount()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetPendingCount() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockConnection.getPendingCount()).andReturn(1276);

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.getPendingCount(), is(1276));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#getServerName()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetServerName() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.getServerName(), is("localhost:27017"));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#isIdle()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testIsIdle() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockConnection.isIdle()).andReturn(false);

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.isIdle(), is(false));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#connection(Server)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testConnection() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        assertThat(conn.connection(server), is(mockConnection));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for
     * {@link ShardedConnection#addPropertyChangeListener(PropertyChangeListener)}
     * and
     * {@link ShardedConnection#removePropertyChangeListener(PropertyChangeListener)}
     * .
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testListeners() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final PropertyChangeListener mockListener = createMock(PropertyChangeListener.class);
        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockListener, mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        conn.addPropertyChangeListener(mockListener);
        conn.removePropertyChangeListener(mockListener);

        verify(mockListener, mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockListener, mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockListener, mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockListener, mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link AbstractProxyMultipleConnection#raiseErrors}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testRaiseErrors() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final MongoDbException error = new MongoDbException();

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        mockConnection.raiseErrors(error);
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        conn.raiseErrors(error);

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#reconnectMain()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testReconnectMain() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final Server server2 = cluster.add("localhost:27018");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockSelector.pickServers()).andReturn(Arrays.asList(server2));
        expect(mockFactory.connect(server2, config)).andReturn(mockConnection2);

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final ConnectionInfo<Server> connInfo = conn.reconnectMain();
        assertThat(connInfo.getConnection(), is(mockConnection2));
        assertThat(connInfo.getConnectionKey(), is(server2));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#reconnectMain()}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testReconnectMainThrows() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");
        final Server server2 = cluster.add("localhost:27018");

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        expect(mockSelector.pickServers()).andReturn(
                Arrays.asList(server, server2));
        expect(mockFactory.connect(server, config)).andThrow(new IOException());
        expect(mockFactory.connect(server2, config)).andReturn(mockConnection2);

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        final ConnectionInfo<Server> connInfo = conn.reconnectMain();
        assertThat(connInfo.getConnection(), is(mockConnection2));
        assertThat(connInfo.getConnectionKey(), is(server2));

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#shutdown(boolean)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testShutdown() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        conn.shutdown(true);

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#waitForClosed(int, TimeUnit)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testWaitForClosed() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        mockConnection.waitForClosed(1, TimeUnit.MILLISECONDS);
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        conn.waitForClosed(1, TimeUnit.MILLISECONDS);

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

    /**
     * Test method for {@link ShardedConnection#waitForClosed(int, TimeUnit)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testWaitForClosedWithZeroDeadline() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ServerSelector mockSelector = createMock(ServerSelector.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<PropertyChangeListener> listener = new Capture<PropertyChangeListener>();

        mockConnection.addPropertyChangeListener(capture(listener));
        expectLastCall();

        replay(mockConnection, mockSelector, mockFactory);

        final ShardedConnection conn = new ShardedConnection(mockConnection,
                server, cluster, mockSelector, mockFactory, config);

        conn.waitForClosed(0, TimeUnit.MILLISECONDS);

        verify(mockConnection, mockSelector, mockFactory);

        // For close.
        reset(mockConnection, mockSelector, mockFactory);
        mockConnection.removePropertyChangeListener(listener.getValue());
        expectLastCall();
        mockConnection.close();

        replay(mockConnection, mockSelector, mockFactory);
        conn.close();
        verify(mockConnection, mockSelector, mockFactory);
    }

}
