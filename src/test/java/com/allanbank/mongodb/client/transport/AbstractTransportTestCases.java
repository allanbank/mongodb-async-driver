/*
 * #%L
 * AbstractTransportTestCases.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2015 Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.transport;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.MockSocketServer;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AbstractTransportTestCases provides tests cases for the {@link Transport}
 * implementations.
 * 
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractTransportTestCases {

    /** The decoder cache for the tests. */
    protected static StringDecoderCache ourDecoderCache;

    /** The encoder cache for the tests. */
    protected static StringEncoderCache ourEncoderCache;

    /** The mock server to connect to. */
    protected static MockSocketServer ourMockServer;

    /**
     * The <code>{ "hello" : "world" }</code> document from the BSON
     * specification.
     */
    private static final byte[] ourHelloWorld = new byte[] { 0x16, 0x00, 0x00,
            0x00, 0x02, (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l',
            (byte) 'o', 0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
            (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

    /**
     * Creates the encoder and decoder caches for the tests.
     */
    @BeforeClass
    public static void createCaches() {
        ourEncoderCache = new StringEncoderCache();
        ourDecoderCache = new StringDecoderCache();
    }

    /**
     * Creates the encoder and decoder caches for the tests.
     */
    @AfterClass
    public static void destroyCaches() {
        ourEncoderCache = null;
        ourDecoderCache = null;
    }

    /**
     * Overloaded fail method to support passing the root cause of the failure.
     * 
     * @param message
     *            The message for the failure.
     * @param cause
     *            The cause of the failure.
     */
    public static void fail(String message, Throwable cause) {
        AssertionError error = new AssertionError(message);
        error.initCause(cause);

        throw error;
    }

    /**
     * Creates the encoder and decoder caches for the tests.
     */
    @BeforeClass
    public static void startMockServer() {
        try {
            ourMockServer = new MockSocketServer();

            ourMockServer.start();
        }
        catch (IOException error) {
            fail("Could not start the mock MongoDB server: "
                    + error.getMessage(), error);
        }
    }

    /**
     * Creates the encoder and decoder caches for the tests.
     */
    @AfterClass
    public static void stopMockServer() {
        IOUtils.close(ourMockServer);
        ourMockServer = null;
    }

    /** The configuration for the client. */
    protected MongoClientConfiguration myConfig = null;

    /** A test TransportResponseListener. */
    protected TestTransportResponseListener myListener = null;

    /** The server to connect to. */
    protected Server myServer = null;

    /** A test TransportResponseListener. */
    protected Transport<TransportOutputBuffer> myTestTransport = null;

    /**
     * Connects to the server.
     */
    @SuppressWarnings("unchecked")
    protected void connect() {
        try {
            TransportFactory testFactory = createFactory();
            myTestTransport = (Transport<TransportOutputBuffer>) testFactory
                    .createTransport(myServer, myConfig, ourEncoderCache,
                            ourDecoderCache, myListener);

            myTestTransport.start();
        }
        catch (IOException e) {
            fail(e.getMessage(), e);
        }
    }

    /**
     * Creates the transport factory for the test.
     * 
     * @return The {@link TransportFactory} for the test.
     */
    protected abstract TransportFactory createFactory();

    /**
     * Initialize the test common objects.
     */
    @Before
    public void setUp() {
        InetSocketAddress address = ourMockServer.getInetSocketAddress();

        Cluster cluster = new Cluster(myConfig, ClusterType.STAND_ALONE);

        myConfig = new MongoClientConfiguration(address);
        myServer = cluster.add(address);
        myListener = new TestTransportResponseListener();
    }

    /**
     * Cleans up after the test.
     */
    @After
    public void tearDown() {
        myConfig = null;
        myServer = null;

        IOUtils.close(myTestTransport);
        assertThat(myListener.getCloses().size(), is(1));

        myTestTransport = null;
        myListener = null;

        if (ourMockServer != null) {
            ourMockServer.clear();
            ourMockServer.waitForDisconnect(60000);
        }
    }

    /**
     * Test that the transport can connect to an open socket.
     */
    @Test
    public void testCanConnect() {
        connect();
        assertThat(ourMockServer.waitForClient(10, SECONDS), is(true));
    }

    /**
     * Test that the transport can handle the server disconnecting.
     */
    @Test
    public void testHandleServerDisconnect() {

        connect();
        assertThat(ourMockServer.waitForClient(10, SECONDS), is(true));

        ourMockServer.disconnectClient();
        assertThat(ourMockServer.waitForDisconnect(10, SECONDS), is(true));

        myListener.waitForClose(10, SECONDS);
        assertThat(myListener.getCloses(), hasSize(1));
    }

    /**
     * Test that the transport can handle the server disconnecting.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testHandleServerDisconnectAfterHeader() throws IOException {
        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + ourHelloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array(), 0, byteBuff.array().length);
        out.write(ourHelloWorld, 0, ourHelloWorld.length / 2);
        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(10, SECONDS), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(1, new GetLastError("db", Durability.ACK), null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        // The message is incomplete so should not get here.
        myListener.waitForResponse(50, MILLISECONDS);

        ourMockServer.disconnectClient();
        assertThat(ourMockServer.waitForDisconnect(10, SECONDS), is(true));

        myListener.waitForClose(10, SECONDS);
        assertThat(myListener.getCloses(), hasSize(1));
    }

    /**
     * Test that the transport can handle the server disconnecting.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testHandleServerDisconnectInHeader() throws IOException {
        final ByteBuffer byteBuff = ByteBuffer.allocate((9 * 4) - (2 * 4));
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + ourHelloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array(), 0, byteBuff.array().length);
        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(10, SECONDS), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(1, new GetLastError("db", Durability.ACK), null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        // The message is incomplete so should not get here.
        myListener.waitForResponse(50, MILLISECONDS);

        ourMockServer.disconnectClient();
        assertThat(ourMockServer.waitForDisconnect(10, SECONDS), is(true));

        myListener.waitForClose(10, SECONDS);
        assertThat(myListener.getCloses(), hasSize(1));
    }

    /**
     * Test that the transport can handle the server disconnecting.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testHandleServerDisconnectInHeaderLength() throws IOException {

        final ByteBuffer byteBuff = ByteBuffer.allocate(4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + ourHelloWorld.length));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array(), 0, byteBuff.array().length - 1);
        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(10, SECONDS), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(1, new GetLastError("db", Durability.ACK), null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        // The message is incomplete so should not get here.
        myListener.waitForResponse(50, MILLISECONDS);

        ourMockServer.disconnectClient();
        assertThat(ourMockServer.waitForDisconnect(10, SECONDS), is(true));

        myListener.waitForClose(10, SECONDS);
        assertThat(myListener.getCloses(), hasSize(1));
    }

    /**
     * Test that the transport can handle sending and receiving a
     * {@link Command} (which is read as a {@link Query}).
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveCommand() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new Command("db", "collection", d().build());
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, instanceOf(Query.class));
    }

    /**
     * Test that the transport can handle sending and receiving a {@link Delete}
     * .
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveDelete() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new Delete("db", "collection", d().build(),
                false);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle sending and receiving a
     * {@link GetMore}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveGetMore() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new GetMore("db", "collection", 101L, 50,
                ReadPreference.PRIMARY);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle sending and receiving an
     * {@link Insert}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveInsert() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new Insert("db", "collection",
                Arrays.asList(d().build()), false);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle sending and receiving a
     * {@link KillCursors}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveKillCursor() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new KillCursors(new long[] { 1L },
                ReadPreference.PRIMARY);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle sending and receiving a {@link Query}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveQuery() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new Query("db", "collection", Find.ALL,
                Find.ALL, 0, 0, 1, false, ReadPreference.PRIMARY, false, false,
                false, false);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle sending and receiving an
     * {@link Update}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSendAndReceiveUpdate() throws IOException {
        final int messageId = 100;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut = new BsonOutputStream(out);

        final Message outMessage = new Update("db", "collection", d().build(),
                d().build(), false, false);
        outMessage.write(messageId, bsonOut);

        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(messageId, outMessage, null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, is(outMessage));
    }

    /**
     * Test that the transport can handle a normal send/receive exchange.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSimpleSendAndReceive() throws IOException {

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + ourHelloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array(), 0, byteBuff.array().length);
        out.write(ourHelloWorld, 0, ourHelloWorld.length);
        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(1, new GetLastError("db", Durability.ACK), null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponse(10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(1));
        TransportInputBuffer inBuffer = myListener.getResponses().get(0);
        Message inMessage = inBuffer.read();
        assertThat(inMessage, instanceOf(Reply.class));
        assertThat(((Reply) inMessage).getResults(), hasSize(1));
        Document reply = ((Reply) inMessage).getResults().get(0);
        assertThat(reply, is(d(e("hello", "world")).build()));
    }

    /**
     * Test that the transport can handle the server responding with two
     * messages for a single send.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testSimpleSendAndReceiveTwo() throws IOException {

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + ourHelloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array(), 0, byteBuff.array().length);
        out.write(ourHelloWorld, 0, ourHelloWorld.length);
        out.write(byteBuff.array(), 0, byteBuff.array().length);
        out.write(ourHelloWorld, 0, ourHelloWorld.length);
        ourMockServer.setReplies(Arrays.asList(out.toByteArray()));

        connect();
        assertThat(ourMockServer.waitForClient(SECONDS.toMillis(5)), is(true));

        TransportOutputBuffer outBuffer = myTestTransport.createSendBuffer(0);
        outBuffer.write(1, new GetLastError("db", Durability.ACK), null);
        myTestTransport.send(outBuffer);
        myTestTransport.flush();
        assertThat(ourMockServer.waitForRequest(1, 10, SECONDS), is(true));

        myListener.waitForResponses(2, 10, SECONDS);
        assertThat(myListener.getResponses(), hasSize(2));
        for (TransportInputBuffer inBuffer : myListener.getResponses()) {
            Message inMessage = inBuffer.read();
            assertThat(inMessage, instanceOf(Reply.class));
            assertThat(((Reply) inMessage).getResults(), hasSize(1));
            Document reply = ((Reply) inMessage).getResults().get(0);
            assertThat(reply, is(d(e("hello", "world")).build()));
        }
    }

}
