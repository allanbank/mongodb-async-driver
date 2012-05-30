/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Tests for the {@link BsonWriter} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnbufferedBsonWriterTest {

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
        builder.addDBPointer("DBPointer", "db", "collection", new ObjectId(1,
                2L));
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
        builder.addObjectId("object-id",
                new ObjectId((int) System.currentTimeMillis() / 1000, 1234L));
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
        aBuilder.addObjectId(new ObjectId(
                (int) System.currentTimeMillis() / 1000, 1234L));
        aBuilder.addRegularExpression(".*", "");
        aBuilder.addString("string");
        aBuilder.addSymbol("symbol");
        aBuilder.addTimestamp(System.currentTimeMillis());
        aBuilder.push().addBoolean("true", true).pop();

        final Document doc = builder.get();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final UnbufferedBsonWriter writer = new UnbufferedBsonWriter(out);

        writer.write(doc);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final BsonReader reader = new BsonReader(in);

        final Document read = reader.readDocument();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);
        
        reader.close();
    }

    /**
     * Test method for {@link BsonWriter#write}.
     * 
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testWriteHelloWorldDocument() throws IOException {
        // From the BSON specification.
        final byte[] helloWorld = new byte[] { 0x16, 0x00, 0x00, 0x00, 0x02,
                (byte) 'h', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o',
                0x00, 0x06, 0x00, 0x00, 0x00, (byte) 'w', (byte) 'o',
                (byte) 'r', (byte) 'l', (byte) 'd', 0x00, 0x00 };

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("hello", "world");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final UnbufferedBsonWriter writer = new UnbufferedBsonWriter(out);

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
        final byte[] arrayDocument = new byte[] { '1', 0x00, 0x00, 0x00, 0x04,
                'B', 'S', 'O', 'N', 0x00, '&', 0x00, 0x00, 0x00, 0x02, '0',
                0x00, 0x08, 0x00, 0x00, 0x00, 'a', 'w', 'e', 's', 'o', 'm',
                'e', 0x00, 0x01, '1', 0x00, '3', '3', '3', '3', '3', '3', 0x14,
                '@', 0x10, '2', 0x00, (byte) 0xc2, 0x07, 0x00, 0x00, 0x00, 0x00 };

        // Expected: { "BSON": ["awesome", 5.05, 1986] }
        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder aBuilder = builder.pushArray("BSON");
        aBuilder.addString("awesome");
        aBuilder.addDouble(5.05);
        aBuilder.addInteger(1986);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final UnbufferedBsonWriter writer = new UnbufferedBsonWriter(out);

        writer.write(builder.get());

        assertArrayEquals(
                " { 'BSON': ['awesome', 5.05, 1986] } not the expected bytes.",
                arrayDocument, out.toByteArray());
    }

}
