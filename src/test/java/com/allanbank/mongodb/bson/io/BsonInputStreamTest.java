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
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.bson.impl.RootDocument;

/**
 * Tests for the BsonInputStream class.
 *
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonInputStreamTest {

    /** A test UUID with a very clear byte pattern. */
    public static final UUID TEST_UUID = new UUID(0x0011223344556677L,
            0x8899aabbccddeeffL);

    /** The legacy byte order for the TEST_UUID. */
    private final static byte[] LEGACY_UUID_BYTES = new byte[] { (byte) 0x77,
        (byte) 0x66, (byte) 0x55, (byte) 0x44, (byte) 0x33, (byte) 0x22,
        (byte) 0x11, (byte) 0x00, (byte) 0xff, (byte) 0xee, (byte) 0xdd,
        (byte) 0xcc, (byte) 0xbb, (byte) 0xaa, (byte) 0x99, (byte) 0x88 };

    /** The standard byte order for the TEST_UUID. */
    private final static byte[] STANDARD_UUID_BYTES = new byte[] { (byte) 0x00,
        (byte) 0x11, (byte) 0x22, (byte) 0x33, (byte) 0x44, (byte) 0x55,
        (byte) 0x66, (byte) 0x77, (byte) 0x88, (byte) 0x99, (byte) 0xaa,
        (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff };

    /**
     * Test method for {@link BsonInputStream#available()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testAvailable() throws IOException {

        final Document seed = BuilderFactory.start()
                .addBinary("juuid", (byte) 3, LEGACY_UUID_BYTES)
                .addBinary("uuid", (byte) 4, STANDARD_UUID_BYTES).build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        bout.writeDocument(seed);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final BsonInputStream reader = new BsonInputStream(in);

        assertEquals(out.toByteArray().length, reader.available());

        reader.close();
    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
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
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

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
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testReadBadUuid() throws IOException {

        final Document seed = BuilderFactory.start()
                .addBinary("uuid", (byte) 4, new byte[15]).build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        bout.writeDocument(seed);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

        assertEquals(seed, doc);
    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testReadBigUtf8StringWorldDocument() throws IOException {
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
        final byte[] valueBytes = value.getBytes(BsonInputStream.UTF8);

        // From the BSON specification.
        final byte[] helloWorld = new byte[17 + valueBytes.length];
        helloWorld[0] = (byte) ((17 + valueBytes.length) & 0xFF);
        helloWorld[1] = (byte) (((17 + valueBytes.length) >> 8) & 0xFF);
        helloWorld[2] = (byte) (((17 + valueBytes.length) >> 16) & 0xFF);
        helloWorld[3] = (byte) (((17 + valueBytes.length) >> 24) & 0xFF);
        helloWorld[4] = (byte) 0x02;
        System.arraycopy(valueBytes, 0, helloWorld, 5, valueBytes.length);
        helloWorld[valueBytes.length + 5] = (byte) 0x00;
        helloWorld[valueBytes.length + 6] = (byte) 0x06;
        helloWorld[valueBytes.length + 7] = (byte) 0x00;
        helloWorld[valueBytes.length + 8] = (byte) 0x00;
        helloWorld[valueBytes.length + 9] = (byte) 0x00;
        helloWorld[valueBytes.length + 10] = (byte) 'h';
        helloWorld[valueBytes.length + 11] = (byte) 'e';
        helloWorld[valueBytes.length + 12] = (byte) 'l';
        helloWorld[valueBytes.length + 13] = (byte) 'l';
        helloWorld[valueBytes.length + 14] = (byte) 'o';
        helloWorld[helloWorld.length - 2] = (byte) 0x00;
        helloWorld[helloWorld.length - 1] = (byte) 0x00;

        final ByteArrayInputStream in = new ByteArrayInputStream(helloWorld);
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", doc instanceof RootDocument);
        assertTrue("Should contain a 'hello' element.", doc.contains(value));

        final Iterator<Element> iter = doc.iterator();
        assertTrue("Should contain a single element.", iter.hasNext());
        iter.next();
        assertFalse("Should contain a single element.", iter.hasNext());

        final Element element = doc.get(value);
        assertNotNull("'hello' element should not be null.", element);
        assertTrue("'hello' element should be a StringElement",
                element instanceof StringElement);

        final StringElement worldElement = (StringElement) element;
        assertEquals("'hello' elements value should be 'hello'.", "hello",
                worldElement.getValue());

    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
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
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

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
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testReadThrowsEof() throws IOException {
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
        final byte[] valueBytes = value.getBytes(BsonInputStream.UTF8);

        // From the BSON specification.
        final byte[] helloWorld = new byte[13 + valueBytes.length];
        helloWorld[0] = (byte) ((17 + valueBytes.length) & 0xFF);
        helloWorld[1] = (byte) (((17 + valueBytes.length) >> 8) & 0xFF);
        helloWorld[2] = (byte) (((17 + valueBytes.length) >> 16) & 0xFF);
        helloWorld[3] = (byte) (((17 + valueBytes.length) >> 24) & 0xFF);
        helloWorld[4] = (byte) 0x02;
        helloWorld[5] = (byte) 'h';
        helloWorld[6] = (byte) 'e';
        helloWorld[7] = (byte) 'l';
        helloWorld[8] = (byte) 'l';
        helloWorld[9] = (byte) 'o';
        helloWorld[10] = (byte) 0x00;
        helloWorld[11] = (byte) ((1 + valueBytes.length) & 0xFF);
        helloWorld[12] = (byte) (((1 + valueBytes.length) >> 8) & 0xFF);
        helloWorld[13] = (byte) (((1 + valueBytes.length) >> 16) & 0xFF);
        helloWorld[14] = (byte) (((1 + valueBytes.length) >> 24) & 0xFF);
        System.arraycopy(valueBytes, 0, helloWorld, 15, valueBytes.length - 2);

        final ByteArrayInputStream in = new ByteArrayInputStream(helloWorld);
        final BsonInputStream reader = new BsonInputStream(in);
        try {
            reader.readDocument();
        }
        catch (final EOFException good) {
            // Expected.
        }
        finally {
            reader.close();
        }
    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testReadUuid() throws IOException {

        final Document seed = BuilderFactory.start()
                .addBinary("juuid", (byte) 3, LEGACY_UUID_BYTES)
                .addBinary("uuid", (byte) 4, STANDARD_UUID_BYTES).build();

        final Document expected = new RootDocument(new UuidElement("juuid",
                (byte) 3, TEST_UUID), new UuidElement("uuid", (byte) 4,
                        TEST_UUID));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        bout.writeDocument(seed);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final BsonInputStream reader = new BsonInputStream(in);

        final Document doc = reader.readDocument();
        reader.close();

        assertEquals(expected, doc);
    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testReadWriteCompleteDocument() throws IOException {
        DocumentBuilder builder = BuilderFactory.start();

        builder.addBoolean("true", true);
        final Document simple = builder.build();

        builder = BuilderFactory.start();
        builder.add(new BooleanElement("_id", false));
        builder.add("binary", new byte[20]);
        builder.add("binary-2", (byte) 2, new byte[40]);
        builder.add("true", true);
        builder.addBoolean("false", false);
        builder.add("DBPointer", "db", "collection", new ObjectId(1, 2L));
        builder.add(
                "double_with_a_really_long_name_to_force_reading_over_multiple_decodes",
                4884.45345);
        builder.add("simple", simple);
        builder.add("int", 123456);
        builder.addJavaScript("javascript", "function foo() { }");
        builder.addJavaScript("javascript_with_code", "function foo() { }",
                simple);
        builder.add("long", 1234567890L);
        builder.addMaxKey("max");
        builder.addMinKey("min");
        builder.addMongoTimestamp("mongo-time", System.currentTimeMillis());
        builder.addNull("null");
        builder.add("object-id", new ObjectId(
                (int) System.currentTimeMillis() / 1000, 1234L));
        builder.addRegularExpression("regex", ".*", "");
        builder.add("regex2", Pattern.compile(".*"));
        builder.add("string", "string\u0090\ufffe");
        builder.addSymbol("symbol", "symbol");
        builder.addTimestamp("timestamp", System.currentTimeMillis());
        builder.add("timestamp2", new Date());
        builder.push("sub-doc").addBoolean("true", true).pop();

        final ArrayBuilder aBuilder = builder.pushArray("array");
        aBuilder.add(new byte[20]);
        aBuilder.add((byte) 2, new byte[40]);
        aBuilder.add(true);
        aBuilder.add(simple);
        aBuilder.addBoolean(false);
        aBuilder.add("db", "collection", new ObjectId(1, 2L));
        aBuilder.add(4884.45345);
        aBuilder.add(123456);
        aBuilder.addJavaScript("function foo() { }");
        aBuilder.addJavaScript("function foo() { }", simple);
        aBuilder.add(1234567890L);
        aBuilder.addMaxKey();
        aBuilder.addMinKey();
        aBuilder.addMongoTimestamp(System.currentTimeMillis());
        aBuilder.addNull();
        aBuilder.add(new ObjectId((int) System.currentTimeMillis() / 1000,
                1234L));
        aBuilder.addRegularExpression(".*", "");
        aBuilder.addRegularExpression(Pattern.compile(".*"));
        aBuilder.add(Pattern.compile(".*"));
        aBuilder.add("string");
        aBuilder.addSymbol("symbol");
        aBuilder.addTimestamp(System.currentTimeMillis());
        aBuilder.add(new Date());
        aBuilder.push().addBoolean("true", true).pop();

        final Document doc = builder.build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream writer = new BsonOutputStream(out);

        writer.writeDocument(doc);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BsonInputStream reader = new BsonInputStream(in);
        Document read = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);

        out.reset();
        writer.writeDocument(doc);

        in = new ByteArrayInputStream(out.toByteArray());
        reader = new BsonInputStream(in);
        read = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);
    }

    /**
     * /** Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     */
    @Test
    public void testReadWriteLong() throws IOException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 0x0102030405060708L);

        final Document doc = builder.build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream writer = new BsonOutputStream(out);

        writer.writeDocument(doc);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        BsonInputStream reader = new BsonInputStream(in);
        Document read = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);

        out.reset();
        writer.writeDocument(doc);

        in = new ByteArrayInputStream(out.toByteArray());
        reader = new BsonInputStream(in);
        read = reader.readDocument();
        reader.close();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);
    }

    /**
     * Test method for {@link BsonInputStream#readDocument()}.
     *
     * @throws IOException
     *             On a failure reading the test document.
     * @throws ClassNotFoundException
     *             On a failure reading the documents.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSerializeCompleteDocument() throws IOException,
    ClassNotFoundException {
        DocumentBuilder builder = BuilderFactory.start();

        builder.addBoolean("true", true);
        final Document simple = builder.build();

        builder = BuilderFactory.start();
        builder.add(new BooleanElement("_id", false));
        builder.add("binary", new byte[20]);
        builder.add("binary-2", (byte) 2, new byte[40]);
        builder.add("true", true);
        builder.addBoolean("false", false);
        builder.add("DBPointer", "db", "collection", new ObjectId(1, 2L));
        builder.add(
                "double_with_a_really_long_name_to_force_reading_over_multiple_decodes",
                4884.45345);
        builder.add("simple", simple);
        builder.add("int", 123456);
        builder.addJavaScript("javascript", "function foo() { }");
        builder.addJavaScript("javascript_with_code", "function foo() { }",
                simple);
        builder.add("long", 1234567890L);
        builder.addMaxKey("max");
        builder.addMinKey("min");
        builder.addMongoTimestamp("mongo-time", System.currentTimeMillis());
        builder.addNull("null");
        builder.add("object-id", new ObjectId(
                (int) System.currentTimeMillis() / 1000, 1234L));
        builder.addRegularExpression("regex", ".*", "");
        builder.add("regex2", Pattern.compile(".*"));
        builder.add("string", "string\u0090\ufffe");
        builder.addSymbol("symbol", "symbol");
        builder.addTimestamp("timestamp", System.currentTimeMillis());
        builder.add("timestamp2", new Date());
        builder.push("sub-doc").addBoolean("true", true).pop();

        final ArrayBuilder aBuilder = builder.pushArray("array");
        aBuilder.add(new byte[20]);
        aBuilder.add((byte) 2, new byte[40]);
        aBuilder.add(true);
        aBuilder.add(simple);
        aBuilder.addBoolean(false);
        aBuilder.add("db", "collection", new ObjectId(1, 2L));
        aBuilder.add(4884.45345);
        aBuilder.add(123456);
        aBuilder.addJavaScript("function foo() { }");
        aBuilder.addJavaScript("function foo() { }", simple);
        aBuilder.add(1234567890L);
        aBuilder.addMaxKey();
        aBuilder.addMinKey();
        aBuilder.addMongoTimestamp(System.currentTimeMillis());
        aBuilder.addNull();
        aBuilder.add(new ObjectId((int) System.currentTimeMillis() / 1000,
                1234L));
        aBuilder.addRegularExpression(".*", "");
        aBuilder.addRegularExpression(Pattern.compile(".*"));
        aBuilder.add(Pattern.compile(".*"));
        aBuilder.add("string");
        aBuilder.addSymbol("symbol");
        aBuilder.addTimestamp(System.currentTimeMillis());
        aBuilder.add(new Date());
        aBuilder.push().addBoolean("true", true).pop();

        final Document doc = builder.build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream writer = new ObjectOutputStream(out);

        writer.writeObject(doc);

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final ObjectInputStream reader = new ObjectInputStream(in);
        final Object read = reader.readObject();
        reader.close();

        assertTrue("Should be a RootDocument.", read instanceof RootDocument);
        assertEquals("Should equal the orginal document.", doc, read);

    }
}
