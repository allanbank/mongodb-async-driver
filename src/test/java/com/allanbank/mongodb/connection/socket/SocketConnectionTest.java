/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.socket;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.BsonReader;
import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.Reply;

/**
 * SocketConnectionTest provides tests for the {@link SocketConnection} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionTest {

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

	/** The test connection. */
	private SocketConnection myTestConnection = null;

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
		ourServer.clear();
	}

	/**
	 * Test method for {@link SocketConnection#close()}.
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testClose() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.close();

		assertTrue("Should have disconnected from the server.",
				ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
		myTestConnection = null;
	}

	/**
	 * Test method for
	 * {@link SocketConnection#getLastError(String, boolean, boolean, int, int)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testGetLastError() throws IOException {

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addInteger("getlasterror", 1);

		final Document doc = builder.get();

		myTestConnection.getLastError("fo", false, false, 0, 0);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", request.length,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
		assertEquals("Response id should be zero.", 0, asInts.get(2));
		assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
				EndianUtils.swap(asInts.get(3)));

		assertEquals("Flags should be zero.", 0, asInts.get(4));
		assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
				'.', '$', 'c', 'm', 'd', 0 },
				Arrays.copyOfRange(request, 20, 28));
		assertEquals("Number to skip not expected.", 0,
				EndianUtils.swap(asInts.get(7)));
		assertEquals("Number to return not expected.", 1,
				EndianUtils.swap(asInts.get(8)));

		final BsonReader reader = new BsonReader(new ByteArrayInputStream(
				Arrays.copyOfRange(request, (7 * 4) + 8, request.length)));

		final Document sent = reader.readDocument();

		assertEquals("The sent command is not the expected command.", doc, sent);
	}

	/**
	 * Test method for
	 * {@link SocketConnection#getLastError(String, boolean, boolean, int, int)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testGetLastErrorWithFsync() throws IOException {

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addInteger("getlasterror", 1);
		builder.addBoolean("fsync", true);

		final Document doc = builder.get();

		myTestConnection.getLastError("fo", true, false, 0, 0);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", request.length,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
		assertEquals("Response id should be zero.", 0, asInts.get(2));
		assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
				EndianUtils.swap(asInts.get(3)));

		assertEquals("Flags should be zero.", 0, asInts.get(4));
		assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
				'.', '$', 'c', 'm', 'd', 0 },
				Arrays.copyOfRange(request, 20, 28));
		assertEquals("Number to skip not expected.", 0,
				EndianUtils.swap(asInts.get(7)));
		assertEquals("Number to return not expected.", 1,
				EndianUtils.swap(asInts.get(8)));

		final BsonReader reader = new BsonReader(new ByteArrayInputStream(
				Arrays.copyOfRange(request, (7 * 4) + 8, request.length)));

		final Document sent = reader.readDocument();

		assertEquals("The sent command is not the expected command.", doc, sent);
	}

	/**
	 * Test method for
	 * {@link SocketConnection#getLastError(String, boolean, boolean, int, int)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testGetLastErrorWithJ() throws IOException {

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addInteger("getlasterror", 1);
		builder.addBoolean("j", true);

		final Document doc = builder.get();

		myTestConnection.getLastError("fo", false, true, 0, 0);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", request.length,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
		assertEquals("Response id should be zero.", 0, asInts.get(2));
		assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
				EndianUtils.swap(asInts.get(3)));

		assertEquals("Flags should be zero.", 0, asInts.get(4));
		assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
				'.', '$', 'c', 'm', 'd', 0 },
				Arrays.copyOfRange(request, 20, 28));
		assertEquals("Number to skip not expected.", 0,
				EndianUtils.swap(asInts.get(7)));
		assertEquals("Number to return not expected.", 1,
				EndianUtils.swap(asInts.get(8)));

		final BsonReader reader = new BsonReader(new ByteArrayInputStream(
				Arrays.copyOfRange(request, (7 * 4) + 8, request.length)));

		final Document sent = reader.readDocument();

		assertEquals("The sent command is not the expected command.", doc, sent);
	}

	/**
	 * Test method for
	 * {@link SocketConnection#getLastError(String, boolean, boolean, int, int)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testGetLastErrorWithW() throws IOException {

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addInteger("getlasterror", 1);
		builder.addInteger("w", 10);
		builder.addInteger("wtimeout", 1000);

		final Document doc = builder.get();

		myTestConnection.getLastError("fo", false, false, 10, 1000);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", request.length,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
		assertEquals("Response id should be zero.", 0, asInts.get(2));
		assertEquals("Wrong OP code.", Operation.QUERY.getCode(),
				EndianUtils.swap(asInts.get(3)));

		assertEquals("Flags should be zero.", 0, asInts.get(4));
		assertArrayEquals("Collection name is wrong.", new byte[] { 'f', 'o',
				'.', '$', 'c', 'm', 'd', 0 },
				Arrays.copyOfRange(request, 20, 28));
		assertEquals("Number to skip not expected.", 0,
				EndianUtils.swap(asInts.get(7)));
		assertEquals("Number to return not expected.", 1,
				EndianUtils.swap(asInts.get(8)));

		final BsonReader reader = new BsonReader(new ByteArrayInputStream(
				Arrays.copyOfRange(request, (7 * 4) + 8, request.length)));

		final Document sent = reader.readDocument();

		assertEquals("The sent command is not the expected command.", doc, sent);
	}

	/**
	 * Test method for
	 * {@link SocketConnection#getMore(String, String, long, int)} .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testGetMore() throws IOException {

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.getMore("foo", "bar", 12345678901234L, 98765);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		assertEquals("Request length is wrong.", (6 * 4) + 8 + 8,
				request.length);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", (6 * 4) + 8 + 8,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#insert(String, String, List, boolean)} .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();
		final List<Document> multi = Arrays.asList(doc, doc);

		myTestConnection.insert("foo", "bar", multi, false);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#insert(String, String, List, boolean)} .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.insert("foo", "bar", Collections.singletonList(doc),
				true);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (5 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for {@link SocketConnection#killCursor(String, String, long)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testKillCursor() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.killCursor("foo", "bar", 12345678901234L);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		assertEquals("Request length is wrong.", (6 * 4) + 8, request.length);

		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", (6 * 4) + 8,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#delete(String, String, Document, boolean)} .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		myTestConnection.delete("foo", "bar", builder.get(), true);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, false, false, false, false, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, false, false, true, false, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, false, false, false, true, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, false, true, false, false, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, false, false, false, false, true);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testQuerySlaveOk() throws IOException {
		// From the BSON specification.
		final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321,
				false, true, false, false, false, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, null, 1234567, 7654321, true,
				false, false, false, false, false);
		myTestConnection.flush();
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));

		final byte[] request = ourServer.getRequests().get(0);
		final int expectedLength = (7 * 4) + 8 + helloWorld.length;
		assertEquals("Request length is wrong.", expectedLength, request.length);
		final IntBuffer asInts = ByteBuffer.wrap(request).asIntBuffer();

		// Header.
		assertEquals("Message size is wrong.", expectedLength,
				EndianUtils.swap(asInts.get(0)));
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#query(String, String, Document, Document, int, int, boolean, boolean, boolean, boolean, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.query("foo", "bar", doc, doc, 7654321, 1234567, false,
				false, false, false, false, false);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for {@link SocketConnection#read()}.
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testRead() throws IOException {
		// From the BSON specification.
		final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");
		final Document doc = builder.get();

		final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
		final IntBuffer buff = byteBuff.asIntBuffer();
		buff.put(0, (7 * 4) + 8 + helloWorld.length);
		buff.put(1, 0);
		buff.put(2, EndianUtils.swap(1234567));
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());
		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.getLastError("fo", false, false, 0, 0);
		myTestConnection.flush();
		// Wake up the server.
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
		final Reply reply = myTestConnection.read();

		final Reply expected = new Reply(1234567, 0, 0, 0,
				Collections.singletonList(doc));

		assertEquals("Did not receive the expected reply.", expected, reply);
	}

	/**
	 * Test method for {@link SocketConnection#read()}.
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test(expected = MongoDbException.class)
	public void testReadNonReply() throws IOException {
		// From the BSON specification.
		final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
		final IntBuffer buff = byteBuff.asIntBuffer();
		buff.put(0, (7 * 4) + 8 + helloWorld.length);
		buff.put(1, 0);
		buff.put(2, EndianUtils.swap(1234567));
		buff.put(3, EndianUtils.swap(Operation.INSERT.getCode()));
		buff.put(4, EndianUtils.swap(0xFF));
		buff.put(5, EndianUtils.swap(123456));
		buff.put(6, 0);
		buff.put(7, EndianUtils.swap(654321));
		buff.put(8, EndianUtils.swap(1));

		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(byteBuff.array());
		out.write(helloWorld);
		ourServer.setReplies(Arrays.asList(out.toByteArray()));

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());
		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.getLastError("fo", false, false, 0, 0);
		myTestConnection.flush();
		// Wake up the server.
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
		myTestConnection.read();
	}

	/**
	 * Test method for {@link SocketConnection#read()}.
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testReadStuff() throws IOException {
		// From the BSON specification.
		final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");
		final Document doc = builder.get();

		final ByteBuffer byteBuff = ByteBuffer.allocate(9 * 4);
		final IntBuffer buff = byteBuff.asIntBuffer();
		buff.put(0, (7 * 4) + 8 + helloWorld.length);
		buff.put(1, 0);
		buff.put(2, EndianUtils.swap(1234567));
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());
		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.getLastError("fo", false, false, 0, 0);
		myTestConnection.flush();
		// Wake up the server.
		assertTrue("Should receive the request after flush.",
				ourServer.waitForRequest(1, TimeUnit.SECONDS.toMillis(10)));
		final Reply reply = myTestConnection.read();

		final Reply expected = new Reply(1234567, 0xFF, 123456, 654321,
				Collections.singletonList(doc));

		assertEquals("Did not receive the expected reply.", expected, reply);
	}

	/**
	 * Test method for
	 * {@link SocketConnection#delete(String, String, Document, boolean)} .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");
		final Document doc = builder.get();

		myTestConnection.delete("foo", "bar", doc, false);
		assertFalse("Should not receive the request until flush.",
				ourServer.waitForRequest(1, 100));

		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * {@link SocketConnection#SocketConnection(InetSocketAddress, MongoDbConfiguration)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testSocketConnection() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		myTestConnection.close();

		assertTrue("Should have disconnected from the server.",
				ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
		myTestConnection = null;
	}

	/**
	 * Test method for
	 * {@link SocketConnection#SocketConnection(InetSocketAddress, MongoDbConfiguration)}
	 * .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test(expected = SocketException.class)
	public void testSocketConnectionFailure() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		// Force to the wrong port.
		myTestConnection = new SocketConnection(new InetSocketAddress(
				addr.getAddress(), addr.getPort() + 1),
				new MongoDbConfiguration());
	}

	/**
	 * Test method for
	 * {@link SocketConnection#update(String, String, Document, Document, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.update("foo", "bar", doc, doc, false, false);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#update(String, String, Document, Document, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.update("foo", "bar", doc, doc, false, true);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
	 * Test method for
	 * {@link SocketConnection#update(String, String, Document, Document, boolean, boolean)}
	 * .
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

		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		myTestConnection = new SocketConnection(addr,
				new MongoDbConfiguration());

		assertTrue("Should have connected to the server.",
				ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

		final DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		final Document doc = builder.get();

		myTestConnection.update("foo", "bar", doc, doc, true, false);
		myTestConnection.flush();
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
		assertTrue("Request id should not be zero.", asInts.get(1) != 0);
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
}
