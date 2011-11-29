/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Tests for the BsonReader class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonReaderTest {

	/**
	 * Test method for {@link BsonReader#readDocument()}.
	 * 
	 * @throws IOException
	 *             On a failure reading the test document.
	 */
	@Test
	public void testReadArrayDocument() throws IOException {
		// From the BSON specification.
		final byte[] arrayDocument = new byte[] { '1', 0x00, 0x00, 0x00, 0x04,
				'B', 'S', 'O', 'N', 0x00, '&', 0x00, 0x00, 0x00, 0x02, '0',
				0x00, 0x08, 0x00, 0x00, 0x00, 'a', 'w', 'e', 's', 'o', 'm',
				'e', 0x00, 0x01, '1', 0x00, '3', '3', '3', '3', '3', '3', 0x14,
				'@', 0x10, '2', 0x00, (byte) 0xc2, 0x07, 0x00, 0x00, 0x00, 0x00 };

		// Expected: { "BSON": ["awesome", 5.05, 1986] }
		final ByteArrayInputStream in = new ByteArrayInputStream(arrayDocument);
		final BsonReader reader = new BsonReader(in);

		final Document doc = reader.readDocument();

		assertTrue("Should be a RootDocument.", doc instanceof RootDocument);
		assertTrue("Should contain a 'hello' element.", doc.contains("BSON"));

		final Iterator<Element> iter = doc.iterator();
		assertTrue("Should contain a single element.", iter.hasNext());
		iter.next();
		assertFalse("Should contain a single element.", iter.hasNext());

		Element element = doc.get("BSON");
		assertNotNull("'BSON' element should not be null.", element);
		assertTrue("'BSON' element should be a ArrayElement",
				element instanceof ArrayElement);

		final ArrayElement values = (ArrayElement) element;
		final List<Element> entries = values.getEntries();
		assertEquals("Should contain 3 entries in the 'BSON' array.", 3,
				entries.size());

		element = entries.get(0);
		assertNotNull("The first 'BSON' entry should not be null.", element);
		assertTrue("The first 'BSON' entry should be a StringElement",
				element instanceof StringElement);
		assertEquals("The first 'BSON' entry should be  '0'.", "0",
				element.getName());
		assertEquals("The first 'BSON' entry should be  'awesome'.", "awesome",
				((StringElement) element).getValue());

		element = entries.get(1);
		assertNotNull("The second 'BSON' entry should not be null.", element);
		assertTrue("The second 'BSON' entry should be a DoubleElement",
				element instanceof DoubleElement);
		assertEquals("The second 'BSON' entry should be  '1'.", "1",
				element.getName());
		assertEquals("The second 'BSON' entry should be  '5.05'.", 5.05,
				((DoubleElement) element).getValue(), 0.01);

		element = entries.get(2);
		assertNotNull("The third 'BSON' entry should not be null.", element);
		assertTrue("The third 'BSON' entry should be a IntegerElement",
				element instanceof IntegerElement);
		assertEquals("The third 'BSON' entry should be  '2'.", "2",
				element.getName());
		assertEquals("The third 'BSON' entry should be  '1986'.", 1986,
				((IntegerElement) element).getValue());
	}

	/**
	 * Test method for {@link BsonReader#readDocument()}.
	 * 
	 * @throws IOException
	 *             On a failure reading the test document.
	 */
	@Test
	public void testReadHelloWorldDocument() throws IOException {
		// From the BSON specification.
		final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
				(byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
				0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
				(byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

		final ByteArrayInputStream in = new ByteArrayInputStream(helloWorld);
		final BsonReader reader = new BsonReader(in);

		final Document doc = reader.readDocument();

		assertTrue("Should be a RootDocument.", doc instanceof RootDocument);
		assertTrue("Should contain a 'hello' element.", doc.contains("hello"));

		final Iterator<Element> iter = doc.iterator();
		assertTrue("Should contain a single element.", iter.hasNext());
		iter.next();
		assertFalse("Should contain a single element.", iter.hasNext());

		final Element element = doc.get("hello");
		assertNotNull("'hello' element should not be null.", element);
		assertTrue("'hello' element should be a StringElement",
				element instanceof StringElement);

		final StringElement worldElement = (StringElement) element;
		assertEquals("'hello' elements value should be 'world'.", "world",
				worldElement.getValue());

	}

	/**
	 * Test method for {@link BsonReader#readDocument()}.
	 * 
	 * @throws IOException
	 *             On a failure reading the test document.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void testReadWriteCompleteDocument() throws IOException {
		DocumentBuilder builder = BuilderFactory.start();

		builder.addBoolean("true", true);
		final Document simple = builder.get();

		builder = BuilderFactory.start();
		builder.addBinary("binary", new byte[20]);
		builder.addBinary("binary-2", (byte) 2, new byte[40]);
		builder.addBoolean("true", true);
		builder.addBoolean("false", false);
		builder.addDBPointer("DBPointer", "db", "collection", new ObjectId(1, 2L));
		builder.addDouble("double", 4884.45345);
		builder.addInteger("int", 123456);
		builder.addJavaScript("javascript", "function foo() { }");
		builder.addJavaScript("javascript_with_code", "function foo() { }",
				simple);
		builder.addLong("long", 1234567890L);
		builder.addMaxKey("max");
		builder.addMinKey("min");
		builder.addMongoTimestamp("mongo-time", System.currentTimeMillis());
		builder.addNull("null");
		builder.addObjectId("object-id", new ObjectId(
				(int) System.currentTimeMillis() / 1000, 1234L));
		builder.addRegularExpression("regex", ".*", "");
		builder.addString("string", "string");
		builder.addSymbol("symbol", "symbol");
		builder.addTimestamp("timestamp", System.currentTimeMillis());
		builder.push("sub-doc").addBoolean("true", true).pop();

		final ArrayBuilder aBuilder = builder.pushArray("array");
		aBuilder.addBinary(new byte[20]);
		aBuilder.addBinary((byte) 2, new byte[40]);
		aBuilder.addBoolean(true);
		aBuilder.addBoolean(false);
		aBuilder.addDBPointer("db", "collection", new ObjectId(1, 2L));
		aBuilder.addDouble(4884.45345);
		aBuilder.addInteger(123456);
		aBuilder.addJavaScript("function foo() { }");
		aBuilder.addJavaScript("function foo() { }", simple);
		aBuilder.addLong(1234567890L);
		aBuilder.addMaxKey();
		aBuilder.addMinKey();
		aBuilder.addMongoTimestamp(System.currentTimeMillis());
		aBuilder.addNull();
		aBuilder.addObjectId(new ObjectId((int) System.currentTimeMillis() / 1000, 1234L));
		aBuilder.addRegularExpression(".*", "");
		aBuilder.addString("string");
		aBuilder.addSymbol("symbol");
		aBuilder.addTimestamp(System.currentTimeMillis());
		aBuilder.push().addBoolean("true", true).pop();

		final Document doc = builder.get();

		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final BsonWriter writer = new BsonWriter(out);

		writer.write(doc);

		final ByteArrayInputStream in = new ByteArrayInputStream(
				out.toByteArray());
		final BsonReader reader = new BsonReader(in);

		final Document read = reader.readDocument();

		assertTrue("Should be a RootDocument.", read instanceof RootDocument);
		assertEquals("Should equal the orginal document.", doc, read);
	}
}
