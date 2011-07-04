/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.BsonWriter;

/**
 * Tests for the {@link BsonWriter} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonWriterTest {

	/**
	 * Test method for {@link BsonWriter#write}.
	 * 
	 * @throws IOException
	 *             On a failure reading the test document.
	 */
	@Test
	public void testWriteHelloWorldDocument() throws IOException {
		// From the BSON specification.
		byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		DocumentBuilder builder = BuilderFactory.start();
		builder.addString("hello", "world");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BsonWriter writer = new BsonWriter(out);

		writer.write(builder.get());

		assertArrayEquals("{ 'hello' : 'world' } not the expected bytes.",
				helloWorld, out.toByteArray());
	}

	/**
	 * Test method for {@link BsonWriter#write}.
	 * 
	 * @throws IOException
	 *             On a failure reading the test document.
	 */
	@Test
	public void tesWriteArrayDocument() throws IOException {
		// From the BSON specification.
		byte[] arrayDocument = new byte[] { '1', 0x00, 0x00, 0x00, 0x04, 'B',
				'S', 'O', 'N', 0x00, '&', 0x00, 0x00, 0x00, 0x02, '0', 0x00,
				0x08, 0x00, 0x00, 0x00, 'a', 'w', 'e', 's', 'o', 'm', 'e',
				0x00, 0x01, '1', 0x00, '3', '3', '3', '3', '3', '3', 0x14, '@',
				0x10, '2', 0x00, (byte) 0xc2, 0x07, 0x00, 0x00, 0x00, 0x00 };

		// Expected: { "BSON": ["awesome", 5.05, 1986] }
		DocumentBuilder builder = BuilderFactory.start();
		ArrayBuilder aBuilder = builder.pushArray("BSON");
		aBuilder.addString("awesome");
		aBuilder.addDouble(5.05);
		aBuilder.addInteger(1986);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BsonWriter writer = new BsonWriter(out);

		writer.write(builder.get());

		assertArrayEquals(
				" { 'BSON': ['awesome', 5.05, 1986] } not the expected bytes.",
				arrayDocument, out.toByteArray());
	}

}
