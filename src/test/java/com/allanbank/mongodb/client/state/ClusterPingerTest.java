/*
 * #%L
 * ClusterPingerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.makeThreadSafe;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.CallbackCapture;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.CallbackReply;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.ReplicaSetStatus;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * ClusterPingerTest provides tests for the {@link ClusterPinger} class.
 * 
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterPingerTest {
    /** The pinger being tested. */
    protected ClusterPinger myPinger = null;

    /**
     * Cleans up the pinger.
     */
    @After
    public void tearDown() {
        IOUtils.close(myPinger);
        myPinger = null;
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweep() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(reply));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepCannotGiveBackConnection() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class),
                cbAndCloseWithConn(reply));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepFails() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(new MongoDbException(
                "Error")));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepReplicaSet() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.REPLICA_SET);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(reply));
        expectLastCall();
        mockConnection.send(anyObject(ReplicaSetStatus.class), cb(reply));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepThrowsIOException() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andThrow(
                new IOException("Injected- 4"));

        replay(mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepThrowsMongoDdException() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class),
                anyObject(ServerUpdateCallback.class));
        expectLastCall().andThrow(new MongoDbException("Injected - 5"));
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.initialSweep(cluster);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep(Cluster)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep in the test.
     */
    @Test
    public void testInitialSweepWhenInterrupted() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        final Capture<ServerUpdateCallback> catureReply = new Capture<ServerUpdateCallback>();

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), capture(catureReply));
        expectLastCall();
        mockConnection.shutdown(false);
        expectLastCall();
        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        final Thread t = new Thread() {
            @Override
            public void run() {
                myPinger.initialSweep(cluster);
            }
        };
        t.start();
        Thread.sleep(20);
        t.interrupt();
        Thread.sleep(50);

        catureReply.getValue().callback(reply(reply));

        t.join(10000);

        verify(mockConnection, mockFactory);

        assertFalse(t.isAlive());

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRun() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunBadPingReply() throws IOException {

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose());
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunCannotGiveConnectionBack() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class),
                cbAndCloseWithConn(reply));
        expectLastCall();

        // Have to shutdown the connection since state won't accept it.
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunInThread() throws IOException, InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(30);
        myPinger.start();
        Thread.sleep(45);
        myPinger.stop();
        Thread.sleep(45);

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunNoTags() throws IOException {

        final DocumentBuilder reply = BuilderFactory.start();
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose(reply));
        expectLastCall();

        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunPingFails() throws IOException {

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndCloseError());
        expectLastCall();

        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwice() throws IOException, InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final InetSocketAddress addr = new InetSocketAddress("localhost", 27017);
        final String address = ServerNameUtils.normalize(addr);

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        cluster.myServers.put(address, new Server(addr));

        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        // Second Sweep.
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwiceIdleConnection() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cb(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        // Second Sweep.
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(tags.build(), state.getTags());
        assertThat(state.getAverageLatency(),
                both(greaterThan(0.0)).and(lessThan(100.0)));
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwiceNotGiveBackConnection() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbWithConn(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        // Second Sweep.
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndClose(reply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockFactory, mockConnection);

        assertEquals(tags.build(), state.getTags());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunThrowsIOException() throws IOException {

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andAnswer(
                a(new IOException("Injected - 1")));

        replay(mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunThrowsMongoDbException() throws IOException {

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), cbAndCloseError());
        expectLastCall().andThrow(new MongoDbException("Injected - 2"));
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunWhenInterrupted() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final Cluster cluster = new Cluster(new MongoClientConfiguration(),
                ClusterType.STAND_ALONE);
        final Server state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        final Capture<ServerUpdateCallback> catureReply = new Capture<ServerUpdateCallback>();
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        mockConnection.send(anyObject(IsMaster.class), capture(catureReply));
        expectLastCall();
        mockConnection.shutdown(true);
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, mockFactory,
                new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(20);
        final Thread t = new Thread(myPinger);
        t.start();
        Thread.sleep(50); // Wait on a reply.
        t.interrupt();
        Thread.sleep(10);

        myPinger.stop();
        t.interrupt();

        verify(mockConnection, mockFactory);

        t.join(1000);
        assertFalse(t.isAlive());

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param <C>
     *            The type for the reply.
     * 
     * @param reply
     *            The reply to return.
     * @return The CloseAnswer.
     */
    protected <C> IAnswer<C> a(final C reply) {
        return new CloseAnswer<C>(reply);
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param reply
     *            The reply to throw.
     * @return The CloseAnswer.
     */
    protected IAnswer<Connection> a(final Throwable reply) {
        return new CloseAnswer<Connection>(reply);
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builders
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    protected ReplyCallback cbAndClose(final DocumentBuilder... builders) {
        return cbAndClose(CallbackReply.reply(builders));
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param reply
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    protected ReplyCallback cbAndClose(final Reply reply) {
        EasyMock.capture(new CloseCallbackReply(reply));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param error
     *            The error to provide to the callback.
     * @return The CallbackReply.
     */
    protected ReplyCallback cbAndClose(final Throwable error) {
        EasyMock.capture(new CloseCallbackReply(error));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @return The CallbackReply.
     */
    protected ReplyCallback cbAndCloseError() {
        EasyMock.capture(new CloseCallbackReply(new Throwable("Injected -3")));
        return null;
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param reply
     *            The reply to throw.
     * @return The CloseAnswer.
     */
    protected IAnswer<String> throwA(final Throwable reply) {
        return new CloseAnswer<String>(reply);
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builder
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    private ReplyCallback cbAndCloseWithConn(final DocumentBuilder builder) {
        class CloseCallbackWithSetConnection extends CloseCallbackReply {

            private static final long serialVersionUID = -2458416861114720698L;

            public CloseCallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
            }
        }
        EasyMock.capture(new CloseCallbackWithSetConnection(CallbackReply
                .reply(builder)));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builder
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    private ReplyCallback cbWithConn(final DocumentBuilder builder) {
        class CallbackWithSetConnection extends CallbackCapture<Reply> {

            private static final long serialVersionUID = -2458416861114720698L;

            public CallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
            }
        }
        EasyMock.capture(new CallbackWithSetConnection(CallbackReply
                .reply(builder)));
        return null;
    }

    /**
     * A specialized {@link IAnswer} to close the pinger.
     * 
     * @param <C>
     *            The type for the answer.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final class CloseAnswer<C> implements IAnswer<C> {
        /** The error to provide to the callback. */
        private final Throwable myError;

        /** The reply to provide to the callback. */
        private final C myReply;

        /**
         * Creates a new CallbackReply.
         * 
         * @param reply
         *            The reply to provide to the callback.
         */
        public CloseAnswer(final C reply) {
            myReply = reply;
            myError = null;
        }

        /**
         * Creates a new CallbackReply.
         * 
         * @param error
         *            The error to provide to the callback.
         */
        public CloseAnswer(final Throwable error) {
            myReply = null;
            myError = error;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to throw the error or return the reply.
         * </p>
         */
        @Override
        public C answer() throws Throwable {
            myPinger.close();

            if (myError != null) {
                throw myError;
            }
            return myReply;
        }
    }

    /**
     * A specialized callback reply to close the pinger when a value is set.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class CloseCallbackReply extends CallbackCapture<Reply> {

        /** The serialization version for the class. */
        private static final long serialVersionUID = -5855409833338626339L;

        /**
         * Creates a new CloseCallbackReply.
         * 
         * @param reply
         *            The reply for the callback.
         */
        public CloseCallbackReply(final Reply reply) {
            super(reply);
        }

        /**
         * Creates a new CloseCallbackReply.
         * 
         * @param thrown
         *            The error for the callback.
         */
        public CloseCallbackReply(final Throwable thrown) {
            super(thrown);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to call super and then provide the reply or error to the
         * callback.
         * </p>
         */
        @Override
        public void setValue(final Callback<Reply> value) {
            super.setValue(value);
            myPinger.close();
        }

    }
}
