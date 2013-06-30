/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.json;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.error.JsonParseException;

/**
 * JsonTest provides tests for the {@link Json} static class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonTest {

    /**
     * Test for parsing a document.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testArray() throws IOException, JsonParseException {
        final InputStream in = getClass().getResourceAsStream("test_array.js");
        final Reader r = new InputStreamReader(in, "UTF-8");
        final BufferedReader reader = new BufferedReader(r);

        Json.parse(reader);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testDocument() throws IOException, JsonParseException {
        final InputStream in = getClass().getResourceAsStream("test_doc.js");
        final Reader r = new InputStreamReader(in, "UTF-8");
        final BufferedReader reader = new BufferedReader(r);

        final Document result = Json.parse(reader);

        assertThat(result, instanceOf(Document.class));

        final DocumentBuilder b = BuilderFactory.start();

        b.add("boolean_1", true).add("boolean_2", false).addNull("n")
                .add("int", 1).add("double", 1.0).add("double_1", 1.0e12)
                .add("double_2", 1e-1).add("string", "abc")
                .add("string2", "def").addSymbol("symbol", "ghi");
        b.pushArray("array").add(1).add(1.0).add(1.0e12).add(1e-1).add("abc")
                .add("def").addSymbol("ghi").push().add("int", 1)
                .add("double", 1.0).add("double_1", 1.0e12)
                .add("double_2", 1e-1).add("string", "abc")
                .add("string2", "def").addSymbol("symbol", "ghi");
        b.push("doc").add("int", 1).add("double", 1.0).add("double_1", 1.0e12)
                .add("double_2", 1e-1).add("string", "abc")
                .add("string2", "def").addSymbol("symbol", "ghi")
                .pushArray("array").add(1).add(1.0).add(1.0e12).add(1e-1)
                .add("abc").add("def").addSymbol("ghi");

        assertEquals(b.build(), result);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseBadArray() throws JsonParseException {
        final String doc = " [ + ] ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseBadDocument() throws JsonParseException {
        final String doc = " { : bar } ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseBadDocument2() throws JsonParseException {
        final String doc = " { foo : + } ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseBadDocument3() throws JsonParseException {
        final String doc = " { + : s } ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test parsing a integer value too big for an IntegerElement.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseBigLong() throws ParseException {
        final Object doc = Json.parse("{ a : 12345678901 }");
        assertEquals(BuilderFactory.start().add("a", 12345678901L).build(), doc);
    }

    /**
     * Test parsing a integer value too big for an IntegerElement in an array.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseBigLongInArray() throws ParseException {
        final Object doc = Json.parse("{ a : [ 12345678901 ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(12345678901L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a BinData(..) element.
     * 
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinData() throws IllegalArgumentException,
            UnsupportedEncodingException {
        final String docText = "{ a : BinData( 5, 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==' ) }";

        final Object doc = Json.parse(docText);

        assertEquals(
                BuilderFactory
                        .start()
                        .addBinary(
                                "a",
                                (byte) 5,
                                "The quick brown fox jumped over the lazy dogs."
                                        .getBytes("US-ASCII")).build(), doc);
    }

    /**
     * Test Parsing a BinData(..) element.
     * 
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinDataInArray() throws IllegalArgumentException,
            UnsupportedEncodingException {
        final String docText = "{ a : [ BinData( 5, 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==' )] }";

        final Object doc = Json.parse(docText);

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addBinary(
                (byte) 5,
                "The quick brown fox jumped over the lazy dogs."
                        .getBytes("US-ASCII"));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a DBPointer() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testParseDbPointer() throws JsonParseException {

        final Object doc = Json
                .parse("{ a : DBPointer('db', \"collection\", ObjectId('4e9d87aa5825b60b637815a6'))}");
        assertEquals(
                BuilderFactory
                        .start()
                        .addDBPointer("a", "db", "collection",
                                new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L))
                        .build(), doc);
    }

    /**
     * Test Parsing a DBPointer() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testParseDbPointerInArray() throws JsonParseException {

        final Object doc = Json
                .parse("{ a : [DBPointer('db', \"collection\", ObjectId('4e9d87aa5825b60b637815a6'))]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addDBPointer("db", "collection",
                new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a HexData(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseHexData() throws JsonParseException,
            IllegalArgumentException {
        final String docText = "{ a : HexData( 6, 'cafe' ) }";

        final Object doc = Json.parse(docText);

        assertEquals(
                BuilderFactory
                        .start()
                        .addBinary("a", (byte) 6,
                                new byte[] { (byte) 0xCA, (byte) 0xFE })
                        .build(), doc);
    }

    /**
     * Test Parsing a HexData(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseHexDataInArray() throws JsonParseException,
            IllegalArgumentException {
        final String docText = "{ a : [ HexData( 6, 'cafe' ) ] }";

        final Object doc = Json.parse(docText);

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addBinary((byte) 6,
                new byte[] { (byte) 0xCA, (byte) 0xFE });
        assertEquals(b.build(), doc);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseIncompleteArray() throws JsonParseException {
        final String doc = " [ ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseIncompleteDocument() throws JsonParseException {
        final String doc = " { ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseIncompleteDocument2() throws JsonParseException {
        final String doc = " { a : b ";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODate() throws JsonParseException,
            java.text.ParseException {

        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Object doc = Json.parse("{ a : ISODate('2012-07-14T01:00:00.000') }");
        assertEquals(
                BuilderFactory.start()
                        .add("a", format.parse("2012-07-14T01:00:00.000UTC"))
                        .build(), doc);

        doc = Json.parse("{ a : ISODate('2012-07-14') }");
        assertEquals(
                BuilderFactory.start()
                        .add("a", format.parse("2012-07-14T00:00:00.000UTC"))
                        .build(), doc);
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODateInArray() throws JsonParseException,
            java.text.ParseException {

        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        final Object doc = Json
                .parse("{ a : [ISODate('2012-07-14T01:00:00.000'),ISODate('2012-07-14')] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(format.parse("2012-07-14T01:00:00.000UTC"))
                .add(format.parse("2012-07-14T00:00:00.000UTC"));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKey() throws JsonParseException {

        final Object doc = Json.parse("{ a : MaxKey( ) }");
        assertEquals(BuilderFactory.start().addMaxKey("a").build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyInArray() throws JsonParseException {

        final Object doc = Json.parse("{ a : [MaxKey( )] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMaxKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKey() throws JsonParseException {

        final Object doc = Json.parse("{ a : MinKey() }");
        assertEquals(BuilderFactory.start().addMinKey("a").build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyInArray() throws JsonParseException {

        final Object doc = Json.parse("{ a :[ MinKey() ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMinKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test parsing a integer value too small for an IntegerElement.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseNegativeLong() throws ParseException {
        final Object doc = Json.parse("{ a : -12345678901 }");
        assertEquals(BuilderFactory.start().add("a", -12345678901L).build(),
                doc);
    }

    /**
     * Test parsing a integer value too small for an IntegerElement in an array.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseNegativeLongInArray() throws ParseException {
        final Object doc = Json.parse("{ a : [ -12345678901 ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(-12345678901L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test(expected = JsonParseException.class)
    public void testParseNonDocumentOrArray() throws JsonParseException {
        final String doc = "foo: bar";

        Json.parse(doc);

        fail("Should have thrown a JsonParseException.");
    }

    /**
     * Test Parsing a NumberLong(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseNumberLong() throws JsonParseException {

        final Object doc = Json.parse("{ a : NumberLong(\"123456789\") }");
        assertEquals(BuilderFactory.start().add("a", 123456789L).build(), doc);
    }

    /**
     * Test Parsing a NumberLong(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseNumberLongInArray() throws JsonParseException {

        final Object doc = Json.parse("{ a : [ NumberLong(\"123456789\") ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(123456789L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ObjectId(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectId() throws JsonParseException {

        final Object doc = Json
                .parse("{ a : ObjectId('4e9d87aa5825b60b637815a6') }");
        assertEquals(
                BuilderFactory
                        .start()
                        .add("a", new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L))
                        .build(), doc);
    }

    /**
     * Test Parsing a ObjectId(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectIdInArray() throws JsonParseException {

        final Object doc = Json
                .parse("{ a : [ObjectId('4e9d87aa5825b60b637815a6')] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestamp() throws JsonParseException {

        final Object doc = Json.parse("{ a : Timestamp(1000,2) }");
        assertEquals(
                BuilderFactory.start()
                        .addMongoTimestamp("a", 0x0000000100000002L).build(),
                doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestampInArray() throws JsonParseException {

        final Object doc = Json.parse("{ a : [Timestamp(1000,2)] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMongoTimestamp(0x0000000100000002L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test for the {@link Json#serialize(DocumentAssignable)} method.
     * 
     * @throws JsonParseException
     *             On a test failure.
     */
    @Test
    public void testSerialize() throws JsonParseException {

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").push().add("a", 1);

        final String doc = Json.serialize(b);
        assertEquals("{ a : [ { a : 1 } ] }", doc);
    }

}
