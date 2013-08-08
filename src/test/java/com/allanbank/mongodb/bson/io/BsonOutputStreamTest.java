/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Tests for the {@link BsonOutputStream} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonOutputStreamTest {

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
     * 
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testWriteBigUtf8StringWorldDocument() throws IOException {
        final StringBuilder builder = new StringBuilder(128);
        builder.append('\u1234'); // Have no idea but its not ASCII!
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");
        builder.append("Now is the time for all good men to come to the aid...");

        final String value = builder.toString();
        final Document wrote = BuilderFactory.start().add("text", value)
                .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream writer = new BsonOutputStream(out);

        writer.writeDocument(wrote);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", doc instanceof RootDocument);
        assertTrue("Should contain a 'hello' element.", doc.contains("text"));

        final Iterator<Element> iter = doc.iterator();
        assertTrue("Should contain a single element.", iter.hasNext());
        iter.next();
        assertFalse("Should contain a single element.", iter.hasNext());

        final Element element = doc.get("text");
        assertNotNull("'text' element should not be null.", element);
        assertTrue("'text' element should be a StringElement",
                element instanceof StringElement);

        final StringElement worldElement = (StringElement) element;
        assertEquals("'text' elements value should be the big string.", value,
                worldElement.getValue());

    }

    /**
     * Test method for {@link BsonOutputStream#writeDocument}.
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
        final BsonOutputStream writer = new BsonOutputStream(out);

        writer.writeDocument(builder.build());

        assertArrayEquals("{ 'hello' : 'world' } not the expected bytes.",
                helloWorld, out.toByteArray());
    }

    /**
     * Test method for {@link BsonOutputStream#writeDocument}.
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
        final BsonOutputStream writer = new BsonOutputStream(out);

        writer.writeDocument(builder.build());

        assertArrayEquals(
                " { 'BSON': ['awesome', 5.05, 1986] } not the expected bytes.",
                arrayDocument, out.toByteArray());
    }

}
