/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client.connection.socket;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.makeThreadSafe;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.SocketFactory;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.IsMaster;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.error.DocumentToLargeException;
import com.allanbank.mongodb.error.ServerVersionException;

/**
 * SocketConnectionTest provides tests for the {@link SocketConnection} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionTest {
    /** Update document with the "build info". */
    private static final Document BUILD_INFO;

    /** A Mock MongoDB server to connect to. */
    private static MockSocketServer ourServer;

    static {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add(Server.MAX_BSON_OBJECT_SIZE_PROP, Client.MAX_DOCUMENT_SIZE);
        builder.pushArray("versionArray").add(99).add(99).add(99);
        BUILD_INFO = new ImmutableDocument(builder);
    }

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        ourServer = new MockSocketServer();
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

    /** The test connection. */
    private SocketConnection myTestConnection = null;

    /** The test connection. */
    private Server myTestServer = null;

    /**
     * Sets up a test {@link Server}.
     */
    @Before
    public void setUp() {
        myTestServer = new Cluster(new MongoClientConfiguration())
                .add(ourServer.getInetSocketAddress());

        // Disable the re-request of build information.
        myTestServer.update(BUILD_INFO);
    }

    /**
     * Cleans up the test connection.
     * 
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @After
    public void tearDown() throws IOException {
        if (myTestConnection != null) {
            myTestConnection.close();
        }
        myTestServer = null;
        ourServer.clear();
        ourServer.waitForDisconnect(60000);
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testAutoClose() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {

        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + helloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        config.setMaxIdleTickCount(3);

        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        assertTrue("Should have disconnected from the server.",
                ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));

        assertThat(myTestConnection.isOpen(), is(false));
        assertThat(myTestConnection.isAvailable(), is(false));
    }

    /**
     * Test method for {@link SocketConnection#close()}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @SuppressWarnings("null")
    @Test
    public void testClose() throws IOException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertThat(myTestConnection.getServerName(),
                is(myTestServer.getCanonicalName()));
        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);

        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        myTestConnection.close();

        assertTrue("Should have disconnected from the server.",
                ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));

        assertFalse("Receive thread should have died.", receive.isAlive());
        assertFalse("Connection should be closed.", myTestConnection.isOpen());
        assertThat(myTestConnection.isAvailable(), is(false));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testCommandToNew() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("silverBullet", 1);
        final Document commandDoc = builder.build();

        final Message command = new Command("db", commandDoc,
                ReadPreference.PRIMARY, Version.parse("99.99.99"));

        // Tell the server our max size is actually small.
        builder.reset().pushArray("versionArray").add(1).add(1).add(1);
        myTestServer.update(builder.build());

        // Now the send should fail.
        try {
            myTestConnection.send(command, null);
            fail("Should have thrown a ServerVersionException");
        }
        catch (final ServerVersionException sve) {
            // Good.
            assertThat(sve.getActualVersion(), is(Version.parse("1.1.1")));
            assertThat(sve.getOperation(), is("silverBullet"));
            assertThat(sve.getSentMessage(), is(command));
            assertThat(sve.getRequiredVersion(), is(Version.parse("99.99.99")));
        }
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testConnectionLost() throws IOException, InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);

        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, null);
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        assertTrue(myTestConnection.isIdle());
        assertTrue(myTestConnection.isOpen());
        assertEquals(0, myTestConnection.getPendingCount());

        // Break the connection.
        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        makeThreadSafe(mockListener, true);
        final Capture<PropertyChangeEvent> capture = new Capture<PropertyChangeEvent>();

        mockListener.propertyChange(capture(capture));
        expectLastCall();

        replay(mockListener);

        myTestConnection.addPropertyChangeListener(mockListener);
        ourServer.disconnectClient();
        ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10));
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        // Pause for a beat for the event to get pushed out.
        waitFor(capture);

        verify(mockListener);

        final PropertyChangeEvent evt = capture.getValue();
        assertEquals(Connection.OPEN_PROP_NAME, evt.getPropertyName());
        assertEquals(Boolean.FALSE, evt.getNewValue());
        assertEquals(Boolean.TRUE, evt.getOldValue());

        myTestConnection.removePropertyChangeListener(mockListener);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testGetLastError() throws IOException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);

        final Document doc = builder.build();

        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, null);
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", request.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                '.', '$', 'c', 'm', 'd', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 0,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", -1,
                EndianUtils.swap(asInts.get(8)));

        final BsonInputStream reader = new BsonInputStream(
                new ByteArrayInputStream(Arrays.copyOfRange(request,
                        (7 * 4) + 8, request.length)));

        final Document sent = reader.readDocument();
        reader.close();

        assertEquals("The sent command is not the expected command.", doc, sent);
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testGetLastErrorWithFsync() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);
        builder.addBoolean("fsync", true);

        final Document doc = builder.build();

        final GetLastError error = new GetLastError("fo", true, false, 0, 0);
        myTestConnection.send(error, null);
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", request.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                '.', '$', 'c', 'm', 'd', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 0,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", -1,
                EndianUtils.swap(asInts.get(8)));

        final BsonInputStream reader = new BsonInputStream(
                new ByteArrayInputStream(Arrays.copyOfRange(request,
                        (7 * 4) + 8, request.length)));

        final Document sent = reader.readDocument();
        reader.close();

        assertEquals("The sent command is not the expected command.", doc, sent);
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testGetLastErrorWithJ() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);
        builder.addBoolean("j", true);

        final Document doc = builder.build();

        final GetLastError error = new GetLastError("fo", false, true, 0, 0);
        myTestConnection.send(error, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", request.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                '.', '$', 'c', 'm', 'd', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 0,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", -1,
                EndianUtils.swap(asInts.get(8)));

        final BsonInputStream reader = new BsonInputStream(
                new ByteArrayInputStream(Arrays.copyOfRange(request,
                        (7 * 4) + 8, request.length)));

        final Document sent = reader.readDocument();
        reader.close();

        assertEquals("The sent command is not the expected command.", doc, sent);
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testGetLastErrorWithW() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("getlasterror", 1);
        builder.addInteger("w", 10);
        builder.addInteger("wtimeout", 1000);

        final Document doc = builder.build();

        final GetLastError error = new GetLastError("fo", false, false, 10,
                1000);
        myTestConnection.send(error, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", request.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                '.', '$', 'c', 'm', 'd', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 0,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", -1,
                EndianUtils.swap(asInts.get(8)));

        final BsonInputStream reader = new BsonInputStream(
                new ByteArrayInputStream(Arrays.copyOfRange(request,
                        (7 * 4) + 8, request.length)));

        final Document sent = reader.readDocument();
        reader.close();

        assertEquals("The sent command is not the expected command.", doc, sent);
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testGetMore() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final GetMore getMore = new GetMore("foo", "bar", 12345678901234L,
                98765, ReadPreference.CLOSEST);
        myTestConnection.send(getMore, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        assertEquals("Request length is wrong.", (6 * 4) + 8 + 8,
                request.length);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", (6 * 4) + 8 + 8,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.GET_MORE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Wrong number to return.", 98765,
                EndianUtils.swap(asInts.get(7)));

        assertEquals("Low cursor value is wrong.",
                12345678901234L & 0xFFFFFFFFL,
                EndianUtils.swap(asInts.get(8)) & 0xFFFFFFFFL);
        assertEquals("High cursor value is wrong.",
                12345678901234L >> Integer.SIZE,
                EndianUtils.swap(asInts.get(9)));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testInsertMulti() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();
        final List<Document> multi = Arrays.asList(doc, doc);

        final Insert insert = new Insert("foo", "bar", multi, true);
        myTestConnection.send(insert, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (5 * 4) + 8 + helloWorld.length
                + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.INSERT.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be one.", 1, EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request,
                        (request.length - helloWorld.length)
                                - helloWorld.length, request.length
                                - helloWorld.length));
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testInsertSingle() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Insert insert = new Insert("foo", "bar",
                Collections.singletonList(doc), false);
        myTestConnection.send(insert, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (5 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.INSERT.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testInsertToLarge() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("data", new byte[2048]);

        final Document doc = builder.build();

        final Insert insert = new Insert("db", "c",
                Collections.singletonList(doc), true);

        // Tell the server our max size is actually small.
        myTestServer.update(builder.reset().add("maxBsonObjectSize", 1024)
                .build());

        // Now the send should fail.
        try {
            myTestConnection.send(insert, null);
            fail("Should have thrown a DocumenToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertThat(dtle.getDocument(), is(doc));
            assertThat(dtle.getMaximumSize(), is(1024));
            assertThat(dtle.getSize(), greaterThan(2048));
        }
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testKillCursor() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final KillCursors kill = new KillCursors(
                new long[] { 12345678901234L }, ReadPreference.CLOSEST);
        myTestConnection.send(kill, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        assertEquals("Request length is wrong.", (6 * 4) + 8, request.length);

        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", (6 * 4) + 8,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.KILL_CURSORS.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertEquals("Wrong number of cursors.", 1,
                EndianUtils.swap(asInts.get(5)));

        assertEquals("Low cursor value is wrong.",
                12345678901234L & 0xFFFFFFFFL,
                EndianUtils.swap(asInts.get(6)) & 0xFFFFFFFFL);
        assertEquals("High cursor value is wrong.",
                12345678901234L >> Integer.SIZE,
                EndianUtils.swap(asInts.get(7)));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testMultiDelete() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Delete delete = new Delete("foo", "bar", builder.build(), false);
        myTestConnection.send(delete, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        assertEquals("Request length is wrong.", (6 * 4) + 8
                + helloWorld.length, request.length);
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", (6 * 4) + 8 + helloWorld.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.DELETE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Flags should be zero.", 0,
                EndianUtils.swap(asInts.get(7)));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQuery() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.PRIMARY, false, false, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryAwaitData() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.PRIMARY, false, true, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags not expected.", 1 << 5,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryExhaust() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.PRIMARY, false, false, true,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags not expected.", 1 << 6,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryNoCursorTimeout() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.PRIMARY, true, false, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags not expected.", 1 << 4,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryPartial() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.PRIMARY, false, false, false,
                true);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();
        // Header.

        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags not expected.", 1 << 7,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQuerySecondaryPreference() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, false, ReadPreference.SECONDARY, false, false, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags not expected.", 1 << 2,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryTailable() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, null, 1234567, 0,
                7654321, true, ReadPreference.PRIMARY, false, false, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 1 << 1,
                EndianUtils.swap(asInts.get(4)));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 7654321,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 1234567,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testQueryWithFields() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Query query = new Query("foo", "bar", doc, doc, 7654321, 0,
                1234567, false, ReadPreference.PRIMARY, false, false, false,
                false);
        myTestConnection.send(query, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (7 * 4) + 8 + helloWorld.length
                + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Flags should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Number to skip not expected.", 1234567,
                EndianUtils.swap(asInts.get(7)));
        assertEquals("Number to return not expected.", 7654321,
                EndianUtils.swap(asInts.get(8)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request,
                        (request.length - helloWorld.length)
                                - helloWorld.length, request.length
                                - helloWorld.length));
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#raiseErrors(MongoDbException)} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRaiseError() throws IOException, InterruptedException {

        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, (7 * 4) + 8 + helloWorld.length);
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.shutdown(false);
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        assertFalse(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());

        myTestConnection.raiseErrors(new MongoDbException());
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testRead() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");
        final Document doc = builder.build();

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + helloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        final Message reply = future.get(60, TimeUnit.SECONDS);

        final Reply expected = new Reply(1, 0, 0,
                Collections.singletonList(doc), false, false, false, false);

        assertEquals("Did not receive the expected reply.", expected, reply);
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testRead2() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");
        final Document doc = builder.build();

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + helloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(2));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        final Message reply = future.get(60, TimeUnit.SECONDS);

        final Reply expected = new Reply(2, 0, 0,
                Collections.singletonList(doc), false, false, false, false);

        assertEquals("Did not receive the expected reply.", expected, reply);
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadDelete() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final Delete deleteMsg = new Delete("db", "collection", Find.ALL, false);
        deleteMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(MongoDbException.class));
            assertThat(te.getCause().getCause(),
                    instanceOf(StreamCorruptedException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     * @throws TimeoutException
     *             On a failure to sleep.
     */
    @SuppressWarnings("null")
    @Test
    public void testReadGarbage() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final Insert insertMsg = new Insert("db", "collection",
                Collections.singletonList(Find.ALL), false);
        insertMsg.write(0, bout);

        final byte[] msgBytes = out.toByteArray();
        msgBytes[14] = (byte) 200; // Mutate the op code.

        ourServer.setReplies(Arrays.asList(msgBytes));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(1000);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);

        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10));
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        try {
            future.get(1, TimeUnit.MINUTES);
            fail("Should have received a failure.");
        }
        catch (final ExecutionException ee) {
            // Good.
            assertThat(ee.getCause(), instanceOf(MongoDbException.class));
        }

        // Pause for everything to cleanup.
        Thread.sleep(100);

        assertTrue(myTestConnection.isIdle());
        assertFalse("Receive thread should have died.", receive.isAlive());
        assertFalse(myTestConnection.isOpen());
        assertEquals(0, myTestConnection.getPendingCount());
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadGetMore() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final GetMore getMoreMsg = new GetMore("db", "collection", 123456, 1,
                ReadPreference.PRIMARY);
        getMoreMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(MongoDbException.class));
            assertThat(te.getCause().getCause(),
                    instanceOf(StreamCorruptedException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadInsert() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final Insert insertMsg = new Insert("db", "collection",
                Collections.singletonList(Find.ALL), false);
        insertMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(MongoDbException.class));
            assertThat(te.getCause().getCause(),
                    instanceOf(StreamCorruptedException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadKillCursors() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final KillCursors killMsg = new KillCursors(new long[] { 1234 },
                ReadPreference.PRIMARY);
        killMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(MongoDbException.class));
            assertThat(te.getCause().getCause(),
                    instanceOf(StreamCorruptedException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadQuery() throws IOException, InterruptedException,
            TimeoutException, ExecutionException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final Query queryMsg = new Query("db", "collection", Find.ALL, null, 1,
                1, 1, false, ReadPreference.PRIMARY, false, false, false, false);
        queryMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(ConnectionLostException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadStuff() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");
        final Document doc = builder.build();

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + helloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, EndianUtils.swap(0xFF));
        buff.put(5, EndianUtils.swap(123456));
        buff.put(6, 0);
        buff.put(7, EndianUtils.swap(654321));
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        final Message reply = future.get(60, TimeUnit.SECONDS);

        final Reply expected = new Reply(1, 123456, 654321,
                Collections.singletonList(doc), true, true, true, true);

        assertEquals("Did not receive the expected reply.", expected, reply);
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadTimeout() throws IOException, InterruptedException,
            TimeoutException {

        ourServer.setReplies(Arrays.asList(new byte[] { 1 }));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(250);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        // This should thrown an exception.
        try {
            future.get(60, TimeUnit.SECONDS);
            fail("Should have thrown an execution exception.");
        }
        catch (final ExecutionException e) {
            // Good.
            assertThat(e.getMessage(), containsString("Read timed out"));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadUpdate() throws IOException, InterruptedException,
            TimeoutException {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        final Update updateMsg = new Update("db", "collection", Find.ALL,
                Find.ALL, false, false);
        updateMsg.write(0, bout);

        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Should have timedout waiting for a reply.");
        }
        catch (final ExecutionException te) {
            // Good.
            assertThat(te.getCause(), instanceOf(MongoDbException.class));
            assertThat(te.getCause().getCause(),
                    instanceOf(StreamCorruptedException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws ExecutionException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReadWhenPendingQueueIsCorrupt() throws IOException,
            InterruptedException, ExecutionException, TimeoutException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, EndianUtils.swap((7 * 4) + 8 + helloWorld.length));
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        final GetLastError error = new GetLastError("fo", false, false, 0, 0);
        myTestConnection.send(error, future);
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        // Now remove items from the pending queue.
        final PendingMessage pending = new PendingMessage();
        assertThat(myTestConnection.myPendingQueue.poll(pending), is(true));
        assertThat(pending.getMessageId(), is(1));
        assertThat(pending.getReplyCallback(),
                sameInstance((Callback<Reply>) future));
        assertThat(myTestConnection.myPendingQueue.poll(pending), is(false));

        // Now to add two back...
        final FutureCallback<Reply> future2 = new FutureCallback<Reply>();
        pending.set(1234, error, future2);
        assertThat(myTestConnection.myPendingQueue.offer(pending), is(true));
        pending.set(1235, error, future);
        assertThat(myTestConnection.myPendingQueue.offer(pending), is(true));

        // Add the reply to the first message.
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        // Send another mesage to trigger the errors.
        final FutureCallback<Reply> future3 = new FutureCallback<Reply>();
        myTestConnection.send(error, future3);

        // Wake up the server.
        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
        try {
            future.get(60, TimeUnit.SECONDS);
            fail("Should have thrown an error");
        }
        catch (final ExecutionException e) {
            assertThat(e.getCause(), instanceOf(MongoDbException.class));
        }
        try {
            future2.get(60, TimeUnit.SECONDS);
            fail("Should have thrown an error");
        }
        catch (final ExecutionException e) {
            assertThat(e.getCause(), instanceOf(MongoDbException.class));
        }
        try {
            future3.get(60, TimeUnit.SECONDS);
            fail("Should have thrown an error");
        }
        catch (final ExecutionException e) {
            assertThat(e.getCause(), instanceOf(MongoDbException.class));
        }
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReplyWithLatency() throws IOException,
            InterruptedException, TimeoutException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final PendingMessage message = new PendingMessage();
        message.timestampNow();

        final List<Document> results = Collections.emptyList();
        myTestConnection.reply(new Reply(1, 1, 1, results, false, false, false,
                false), message);
    }

    /**
     * Test method for {@link SocketConnection}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws TimeoutException
     *             On a failure waiting for a reply.
     * @throws InterruptedException
     *             On a failure waiting for a reply.
     */
    @Test
    public void testReplyWithoutLatency() throws IOException,
            InterruptedException, TimeoutException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final PendingMessage message = new PendingMessage();

        final List<Document> results = Collections.emptyList();
        myTestConnection.reply(new Reply(1, 1, 1, results, false, false, false,
                false), message);
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSecondMessageToLarge() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("data", new byte[2048]);

        final Document doc = builder.build();

        final Insert insert = new Insert("db", "c",
                Collections.singletonList(doc), true);

        // Tell the server our max size is actually small.
        myTestServer.update(builder.reset().add("maxBsonObjectSize", 1024)
                .build());

        // Now the send should fail.
        try {
            myTestConnection.send(new IsMaster(), insert, null);
            fail("Should have thrown a DocumenToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertThat(dtle.getDocument(), is(doc));
            assertThat(dtle.getMaximumSize(), is(1024));
            assertThat(dtle.getSize(), greaterThan(2048));
        }
    }

    /**
     * Test method for {@link SocketConnection#close()}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure waiting for the threads to close.
     */
    @SuppressWarnings("null")
    @Test
    public void testSendError() throws IOException, InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        myTestConnection.send(new PoisonMessage(new OutOfMemoryError(
                "injected error")), null);

        // Receive should see the disconnect first.
        receive.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse("Receive thread should have died.", receive.isAlive());
        assertFalse("Connection should be closed.", myTestConnection.isOpen());

        // // myTestConnection = null;
    }

    /**
     * Test method for {@link SocketConnection#close()}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure waiting for the threads to close.
     */
    @SuppressWarnings("null")
    @Test
    public void testSendFailureClose() throws IOException, InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        myTestConnection.send(new PoisonMessage(new IOException(
                "injected error")), null);

        // Receive should see the disconnect first.
        receive.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse("Receive thread should have died.", receive.isAlive());
        assertFalse("Connection should be closed.", myTestConnection.isOpen());

        // myTestConnection = null;
    }

    /**
     * Test method for {@link SocketConnection#close()}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure waiting for the threads to close.
     */
    @SuppressWarnings("null")
    @Test
    public void testSendRuntimeException() throws IOException,
            InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        myTestConnection.send(new PoisonMessage(new MongoDbException(
                "injected error")), null);

        // Receive should see the disconnect first.
        receive.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse("Receive thread should have died.", receive.isAlive());
        assertFalse("Connection should be closed.", myTestConnection.isOpen());

        // myTestConnection = null;
    }

    /**
     * Test method for {@link SocketConnection#close()}.
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure waiting for the threads to close.
     */
    @SuppressWarnings("null")
    @Test
    public void testServerClose() throws IOException, InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);

        Thread receive = null;
        for (final Thread t : threads) {
            if (t.getName().contains("<--")) {
                assertNull("Found 2 receive threads: " + t.getName(), receive);
                receive = t;
            }
        }
        assertNotNull("Did not find the receive thread", receive);

        assertTrue(ourServer.disconnectClient());

        // Receive should see the disconnect first.
        receive.join(TimeUnit.SECONDS.toMillis(600));

        assertFalse(
                "Receive thread should have died: " + receive.getStackTrace(),
                receive.isAlive());
        assertFalse("Connection should be closed.", myTestConnection.isOpen());

        // myTestConnection = null;
    }

    /**
     * Test method for {@link SocketConnection#shutdown} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testShutdown() throws IOException, InterruptedException {

        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, (7 * 4) + 8 + helloWorld.length);
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.shutdown(false);
        assertThat(myTestConnection.isAvailable(), is(false));
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());
    }

    /**
     * Test method for {@link SocketConnection#shutdown} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testShutdownWhenClosed() throws IOException,
            InterruptedException {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.close();
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());

        myTestConnection.shutdown(false);
        assertThat(myTestConnection.isAvailable(), is(false));

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());

    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSingleDelete() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");
        final Document doc = builder.build();

        final Delete delete = new Delete("foo", "bar", doc, true);
        myTestConnection.send(delete, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        assertEquals("Request length is wrong.", (6 * 4) + 8
                + helloWorld.length, request.length);
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", (6 * 4) + 8 + helloWorld.length,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.DELETE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Flags should be one.", 1, EndianUtils.swap(asInts.get(7)));
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSocketConnectFailure() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final SocketFactory mockFactory = createMock(SocketFactory.class);
        final Socket mockSocket = createMock(Socket.class);

        final SocketException thrown = new SocketException("Injected.");
        expect(mockFactory.createSocket()).andReturn(mockSocket);

        mockSocket.connect(ourServer.getInetSocketAddress(),
                config.getConnectTimeout());
        expectLastCall().andThrow(thrown);

        mockSocket.close();
        expectLastCall().andThrow(new IOException("Injected but just logged."));

        replay(mockFactory, mockSocket);

        config.setSocketFactory(mockFactory);
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add(ourServer.getInetSocketAddress());

        try {
            myTestConnection = new SocketConnection(server, config);
            fail("Should have thrown an SocketException");
        }
        catch (final SocketException good) {
            assertThat(good, sameInstance(thrown));
        }

        verify(mockFactory, mockSocket);
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSocketConnection() throws IOException {

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.close();

        assertTrue("Should have disconnected from the server.",
                ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
        // myTestConnection = null;
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test(expected = SocketException.class)
    public void testSocketConnectionFailure() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();

        // Force to the wrong port.
        final Cluster cluster = new Cluster(new MongoClientConfiguration());
        final Server wrongPort = cluster.add(new InetSocketAddress(addr
                .getAddress(), addr.getPort() + 1));
        myTestConnection = new SocketConnection(wrongPort,
                new MongoClientConfiguration());
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSocketFactoryFailure() throws IOException {

        final SocketFactory mockFactory = createMock(SocketFactory.class);

        final IOException thrown = new IOException("Injected.");
        expect(mockFactory.createSocket()).andThrow(thrown);

        replay(mockFactory);

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setSocketFactory(mockFactory);

        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add(ourServer.getInetSocketAddress());

        try {
            myTestConnection = new SocketConnection(server, config);
            fail("Should have thrown an IOException");
        }
        catch (final IOException good) {
            assertThat(good, sameInstance(thrown));
        }

        verify(mockFactory);
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSocketOptionsFailure() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final SocketFactory mockFactory = createMock(SocketFactory.class);
        final Socket mockSocket = createMock(Socket.class);

        final SocketException thrown = new SocketException("Injected.");
        expect(mockFactory.createSocket()).andReturn(mockSocket);

        mockSocket.connect(ourServer.getInetSocketAddress(),
                config.getConnectTimeout());
        expectLastCall();

        mockSocket.setKeepAlive(config.isUsingSoKeepalive());
        expectLastCall();
        mockSocket.setSoTimeout(config.getReadTimeout());
        expectLastCall();
        mockSocket.setTcpNoDelay(true);
        expectLastCall().andThrow(thrown);
        // mockSocket.setPerformancePreferences(1, 5, 6);

        replay(mockFactory, mockSocket);

        config.setSocketFactory(mockFactory);
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add(ourServer.getInetSocketAddress());

        try {
            myTestConnection = new SocketConnection(server, config);
            fail("Should have thrown an SocketException");
        }
        catch (final SocketException good) {
            assertThat(good, sameInstance(thrown));
        }

        verify(mockFactory, mockSocket);
    }

    /**
     * Test method for
     * {@link SocketConnection#SocketConnection(Server, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testSocketOptionsFailureFromUnixDomainSockets()
            throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final SocketFactory mockFactory = createMock(SocketFactory.class);
        final Socket mockSocket = createMock(Socket.class);

        final SocketException thrown = new AFUNIXSocketException();
        expect(mockFactory.createSocket()).andReturn(mockSocket);

        mockSocket.connect(ourServer.getInetSocketAddress(),
                config.getConnectTimeout());
        expectLastCall();

        mockSocket.setKeepAlive(config.isUsingSoKeepalive());
        expectLastCall();
        mockSocket.setSoTimeout(config.getReadTimeout());
        expectLastCall();
        mockSocket.setTcpNoDelay(true);
        expectLastCall().andThrow(thrown);
        mockSocket.setPerformancePreferences(1, 5, 6);
        expectLastCall();
        expect(mockSocket.getInputStream()).andReturn(
                new ByteArrayInputStream(new byte[0]));
        expect(mockSocket.getOutputStream()).andReturn(
                new ByteArrayOutputStream());
        expect(mockSocket.getLocalPort()).andReturn(12345);

        mockSocket.close();
        expectLastCall();

        replay(mockFactory, mockSocket);

        config.setSocketFactory(mockFactory);
        final Cluster cluster = new Cluster(config);
        final Server server = cluster.add(ourServer.getInetSocketAddress());

        myTestConnection = new SocketConnection(server, config);
        myTestConnection.close();
        myTestConnection = null;

        verify(mockFactory, mockSocket);
    }

    /**
     * Test method for {@link SocketConnection#stop} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testStop() throws IOException, InterruptedException {

        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, (7 * 4) + 8 + helloWorld.length);
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.stop();
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testUpdate() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Update update = new Update("foo", "bar", doc, doc, false, false);
        myTestConnection.send(update, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (6 * 4) + 8 + helloWorld.length
                + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.UPDATE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Flags should be zero.", 0, asInts.get(7));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request,
                        (request.length - helloWorld.length)
                                - helloWorld.length, request.length
                                - helloWorld.length));
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testUpdateMulti() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Update update = new Update("foo", "bar", doc, doc, true, false);
        myTestConnection.send(update, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (6 * 4) + 8 + helloWorld.length
                + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.UPDATE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Flags should be two.", 2, EndianUtils.swap(asInts.get(7)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request,
                        (request.length - helloWorld.length)
                                - helloWorld.length, request.length
                                - helloWorld.length));
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#send} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testUpdateUpsert() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        myTestConnection = new SocketConnection(myTestServer,
                new MongoClientConfiguration());
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final Document doc = builder.build();

        final Update update = new Update("foo", "bar", doc, doc, false, true);
        myTestConnection.send(update, null);

        assertTrue("Should receive the request after flush.",
                ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

        final byte[] request = ourServer.getRequests().get(0);
        final int expectedLength = (6 * 4) + 8 + helloWorld.length
                + helloWorld.length;
        assertEquals("Request length is wrong.", expectedLength, request.length);
        final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

        // Header.
        assertEquals("Message size is wrong.", expectedLength,
                EndianUtils.swap(asInts.get(0)));
        assertTrue("Request id should not be one.", asInts.get(1) != 1);
        assertEquals("Response id should be zero.", 0, asInts.get(2));
        assertEquals("Wrong OP code.", Operation.UPDATE.getCode(),
                EndianUtils.swap(asInts.get(3)));

        assertEquals("Reserved should be zero.", 0, asInts.get(4));
        assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
                'o', '.', 'b', 'a', 'r', 0 },
                Arrays.copyOfRange(request, 20, 28));
        assertEquals("Flags should be one.", 1, EndianUtils.swap(asInts.get(7)));

        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request,
                        (request.length - helloWorld.length)
                                - helloWorld.length, request.length
                                - helloWorld.length));
        assertArrayEquals(
                "The end of the request should be the hello world document.",
                helloWorld, Arrays.copyOfRange(request, request.length
                        - helloWorld.length, request.length));
    }

    /**
     * Test method for {@link SocketConnection#shutdown} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testWaitForClosedWhenInterrupted() throws IOException,
            InterruptedException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
        final IntBuffer buff = byteBuff.asIntBuffer();
        buff.put(0, (7 * 4) + 8 + helloWorld.length);
        buff.put(1, 0);
        buff.put(2, EndianUtils.swap(1));
        buff.put(3, EndianUtils.swap(Operation.REPLY.getCode()));
        buff.put(4, 0);
        buff.put(5, 0);
        buff.put(6, 0);
        buff.put(7, 0);
        buff.put(8, EndianUtils.swap(1));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(byteBuff.array());
        out.write(helloWorld);
        ourServer.setReplies(Arrays.asList(out.toByteArray()));

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setReadTimeout(100);
        myTestConnection = new SocketConnection(myTestServer, config);
        myTestConnection.start();

        assertTrue("Should have connected to the server.",
                ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

        myTestConnection.shutdown(false);
        Thread.currentThread().interrupt();
        myTestConnection.waitForClosed(10, TimeUnit.SECONDS);

        assertTrue(myTestConnection.isIdle());
        assertFalse(myTestConnection.isOpen());

    }

    /**
     * Waits for the capture to have been set.
     * 
     * @param capture
     *            The capture to wait for.
     */
    private void waitFor(final Capture<PropertyChangeEvent> capture) {
        long now = System.currentTimeMillis();
        final long deadline = now + TimeUnit.SECONDS.toMillis(10);

        while (!capture.hasCaptured() && (now < deadline)) {
            try {
                // A slow spin loop.
                TimeUnit.MILLISECONDS.sleep(10);
            }
            catch (final InterruptedException e) {
                // Ignore.
                e.hashCode();
            }
            now = System.currentTimeMillis();
        }
    }

    /**
     * AFUNIXSocketException provides a test instance of the Unix domain socket
     * exception.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static final class AFUNIXSocketException extends SocketException {

        /** The serialization id for the class. */
        private static final long serialVersionUID = 1433767421262380441L;

    }
}
