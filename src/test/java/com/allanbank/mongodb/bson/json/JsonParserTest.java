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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * JsonParserTest provides tests for the {@link JsonParser}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonParserTest {

    /**
     * Test for parsing a document.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testArray() throws IOException, ParseException {
        final InputStream in = getClass().getResourceAsStream("test_array.js");
        final Reader r = new InputStreamReader(in, "UTF-8");
        final BufferedReader reader = new BufferedReader(r);

        final StringBuilder builder = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
            builder.append(line).append("\n");

            line = reader.readLine();
        }
        r.close();

        final JsonParser parser = new JsonParser();

        final Object result = parser.parse(builder.toString());

        assertThat(result, instanceOf(List.class));

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.add(false).add(true).addNull().add(1).add(1.0).add(1.0e12).add(1e-1)
                .add("abc").add("def").addSymbol("ghi");

        final DocumentBuilder b = ab.push();
        b.add("int", 1).add("double", 1.0).add("double_1", 1.0e12)
                .add("double_2", 1e-1).add("string", "abc")
                .add("string2", "def").addSymbol("symbol", "ghi");
        b.pushArray("array").add(1).add(1.0).add(1.0e12).add(1e-1).add("abc")
                .add("def").addSymbol("ghi");
        b.push("doc").add("int", 1).add("double", 1.0).add("double_1", 1.0e12)
                .add("double_2", 1e-1).add("string", "abc")
                .add("string2", "def").addSymbol("symbol", "ghi")
                .pushArray("array").add(1).add(1.0).add(1.0e12).add(1e-1)
                .add("abc").add("def").addSymbol("ghi");

        ab.pushArray().add(1).add(1.0).add(1.0e12).add(1e-1).add("abc")
                .add("def").addSymbol("ghi");

        assertEquals(Arrays.asList(ab.build()), result);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testDocument() throws IOException, ParseException {
        final InputStream in = getClass().getResourceAsStream("test_doc.js");
        final Reader r = new InputStreamReader(in, "UTF-8");

        final JsonParser parser = new JsonParser();

        final Object result = parser.parse(r);
        r.close();

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

        assertEquals(JsonParserConstants.TOKEN_CLOSE_BRACE,
                parser.getToken(0).kind);
        assertEquals(JsonParserConstants.EOF, parser.getNextToken().kind);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseBadArray() throws ParseException {
        final String doc = " [ + ] ";

        final JsonParser parser = new JsonParser();

        parser.parse(doc);

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseBadDocument() throws ParseException {
        final String doc = " { : bar } ";

        final JsonParser parser = new JsonParser();
        parser.ReInit(new ByteArrayInputStream(doc.getBytes()));
        parser.parse();

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseBadDocument2() throws ParseException {
        final String doc = " { foo : + } ";

        final JsonParser parser = new JsonParser(new StringReader(doc));

        parser.parse();

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseBadDocument3() throws ParseException {
        final String doc = " { + : s } ";

        final JsonParser parser = new JsonParser(new ByteArrayInputStream(
                doc.getBytes()));

        parser.disable_tracing();
        parser.enable_tracing();
        parser.parse();

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test Parsing a BinData(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinData() throws ParseException,
            IllegalArgumentException, UnsupportedEncodingException {
        final String docText = "{ a : BinData( 5, 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==' ) }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

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
     * @throws ParseException
     *             On a test failure.
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinDataInArray() throws ParseException,
            IllegalArgumentException, UnsupportedEncodingException {
        final String docText = "{ a : [ BinData( 5, 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==' )] }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addBinary(
                (byte) 5,
                "The quick brown fox jumped over the lazy dogs."
                        .getBytes("US-ASCII"));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a BinData(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinDataStrict() throws ParseException,
            IllegalArgumentException, UnsupportedEncodingException {
        final String docText = "{ a : { $binary : 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==', $type : 5 } }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

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
     * @throws ParseException
     *             On a test failure.
     * @throws UnsupportedEncodingException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseBinDataStrictInArray() throws ParseException,
            IllegalArgumentException, UnsupportedEncodingException {
        final String docText = "{ a : [ { $binary : 'VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wZWQgb3ZlciB0aGUgbGF6eSBkb2dzLg==', $type : 5 }] }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addBinary(
                (byte) 5,
                "The quick brown fox jumped over the lazy dogs."
                        .getBytes("US-ASCII"));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testParseDbPointer() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : DBPointer('db', \"collection\", ObjectId('4e9d87aa5825b60b637815a6'))}");
        assertEquals(
                BuilderFactory
                        .start()
                        .addDBPointer("a", "db", "collection",
                                new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L))
                        .build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testParseDbPointerInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : [DBPointer('db', \"collection\", ObjectId('4e9d87aa5825b60b637815a6'))]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addDBPointer("db", "collection",
                new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a HexData(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseHexData() throws ParseException,
            IllegalArgumentException {
        final String docText = "{ a : HexData( 6, 'cafe' ) }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

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
     * @throws ParseException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testParseHexDataInArray() throws ParseException,
            IllegalArgumentException {
        final String docText = "{ a : [ HexData( 6, 'cafe' ) ] }";

        final JsonParser parser = new JsonParser();
        final Object doc = parser.parse(docText);

        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addBinary((byte) 6,
                new byte[] { (byte) 0xCA, (byte) 0xFE });
        assertEquals(b.build(), doc);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseIncompleteArray() throws ParseException {
        final String doc = " [ ";

        final JsonParser parser = new JsonParser();

        parser.parse(doc);

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseIncompleteDocument() throws ParseException {
        final String doc = " { ";

        final JsonParser parser = new JsonParser();

        parser.parse(doc);

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseIncompleteDocument2() throws ParseException {
        final String doc = " { a : b ";

        final JsonParserTokenManager tokenMgr = new JsonParserTokenManager(
                new JavaCharStream(new StringReader(doc)));

        final JsonParser parser = new JsonParser();
        parser.ReInit(tokenMgr);
        parser.parse();

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODate() throws ParseException,
            java.text.ParseException {
        final JsonParser parser = new JsonParser();
        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Object doc = parser.parse("{ a : ISODate('2012-07-14T01:00:00.000') }");
        assertEquals(
                BuilderFactory.start()
                        .add("a", format.parse("2012-07-14T01:00:00.000UTC"))
                        .build(), doc);

        doc = parser.parse("{ a : ISODate('2012-07-14') }");
        assertEquals(
                BuilderFactory.start()
                        .add("a", format.parse("2012-07-14T00:00:00.000UTC"))
                        .build(), doc);
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODateInArray() throws ParseException,
            java.text.ParseException {
        final JsonParser parser = new JsonParser();
        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        final Object doc = parser
                .parse("{ a : [ISODate('2012-07-14T01:00:00.000'),ISODate('2012-07-14')] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(format.parse("2012-07-14T01:00:00.000UTC"))
                .add(format.parse("2012-07-14T00:00:00.000UTC"));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODateInArrayStrict() throws ParseException,
            java.text.ParseException {
        final JsonParser parser = new JsonParser();
        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        final Date expect1 = format.parse("2012-07-14T01:00:00.000UTC");
        final Date expect2 = format.parse("2012-07-14T00:00:00.000UTC");

        Object doc = parser
                .parse("{ a : [{ $date : '2012-07-14T01:00:00.000'},{ $date:'2012-07-14'}] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(expect1).add(expect2);
        assertEquals(b.build(), doc);

        doc = parser.parse("{ a : [{ $date : " + expect1.getTime()
                + "},{ $date:" + expect2.getTime() + "}] }");
        b.reset();
        b.pushArray("a").add(expect1).add(expect2);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ISODate(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     * @throws java.text.ParseException
     *             On a test failure.
     */
    @Test
    public void testParseISODateStrict() throws ParseException,
            java.text.ParseException {
        final JsonParser parser = new JsonParser();
        final SimpleDateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Date expect = format.parse("2012-07-14T01:00:00.000UTC");
        Object doc = parser.parse("{ a : { $date : " + expect.getTime()
                + " } }");
        assertEquals(BuilderFactory.start().add("a", expect).build(), doc);
        doc = parser.parse("{ a : { $date : '2012-07-14T01:00:00.000' } }");
        assertEquals(BuilderFactory.start().add("a", expect).build(), doc);

        expect = format.parse("2012-07-14T00:00:00.000UTC");
        doc = parser.parse("{ a : { $date : " + expect.getTime() + " } }");
        assertEquals(BuilderFactory.start().add("a", expect).build(), doc);
        doc = parser.parse("{ a : { $date : '2012-07-14'} }");
        assertEquals(BuilderFactory.start().add("a", expect).build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKey() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : MaxKey( ) }");
        assertEquals(BuilderFactory.start().addMaxKey("a").build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [MaxKey( )] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMaxKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyInArrayNoParens() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [MaxKey] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMaxKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyInArrayStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [{ $maxKey:1}] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMaxKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyNoParens() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : MaxKey }");
        assertEquals(BuilderFactory.start().addMaxKey("a").build(), doc);
    }

    /**
     * Test Parsing a MaxKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMaxKeyStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : { $maxKey:1} }");
        assertEquals(BuilderFactory.start().addMaxKey("a").build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKey() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : MinKey() }");
        assertEquals(BuilderFactory.start().addMinKey("a").build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a :[ MinKey() ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMinKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyInArrayNoParens() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a :[ MinKey ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMinKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyInArrayStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [{ $minKey:1}] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMinKey();
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyNoParens() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : MinKey}");
        assertEquals(BuilderFactory.start().addMinKey("a").build(), doc);
    }

    /**
     * Test Parsing a MinKey() element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseMinKeyStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : { $minKey:1} }");
        assertEquals(BuilderFactory.start().addMinKey("a").build(), doc);
    }

    /**
     * Test for parsing a document.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test(expected = ParseException.class)
    public void testParseNonDocumentOrArray() throws ParseException {
        final String doc = "foo: bar";

        final JsonParserTokenManager tokenMgr = new JsonParserTokenManager(
                new JavaCharStream(new StringReader(doc)));

        final JsonParser parser = new JsonParser(tokenMgr);
        parser.parse();

        fail("Should have thrown a ParseException.");
    }

    /**
     * Test Parsing a NumberLong(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseNumberLong() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : NumberLong(\"123456789\") }");
        assertEquals(BuilderFactory.start().add("a", 123456789L).build(), doc);
    }

    /**
     * Test Parsing a NumberLong(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseNumberLongInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [ NumberLong(\"123456789\") ]}");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(123456789L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ObjectId(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectId() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
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
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectIdInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : [ObjectId('4e9d87aa5825b60b637815a6')] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ObjectId(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectIdInArrayStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : [ {$oid : '4e9d87aa5825b60b637815a6'}] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").add(new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L));
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a ObjectId(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseObjectIdStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : {$oid : '4e9d87aA5825b60b637815a6'} }");
        assertEquals(
                BuilderFactory
                        .start()
                        .add("a", new ObjectId(0x4e9d87aa, 0x5825b60b637815a6L))
                        .build(), doc);
    }

    /**
     * Test Parsing a {$regex:"<sRegex>","$options": "<sOptions>"} element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseRegularExpressionInArrayStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : [{ $regex : '.*1', $options : '' }, "
                        + "{ $regex : '.*2' }, { $regex : '.*3' , $options : 'i'}] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addRegularExpression(".*1", "")
                .addRegularExpression(".*2", "")
                .addRegularExpression(".*3", "i");
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a {$regex:"<sRegex>","$options": "<sOptions>"} element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseRegularExpressionStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        Object doc = parser.parse("{ a : { $regex : '.*', $options : '' } }");
        assertEquals(BuilderFactory.start().addRegularExpression("a", ".*", "")
                .build(), doc);

        doc = parser.parse("{ a : { $regex : '.*' } }");
        assertEquals(BuilderFactory.start().addRegularExpression("a", ".*", "")
                .build(), doc);

        doc = parser.parse("{ a : { $regex : '.*' , $options : 'i'} }");
        assertEquals(BuilderFactory.start()
                .addRegularExpression("a", ".*", "i").build(), doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestamp() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : Timestamp(1000,2) }");
        assertEquals(
                BuilderFactory.start()
                        .addMongoTimestamp("a", 0x0000000100000002L).build(),
                doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestampInArray() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser.parse("{ a : [Timestamp(1000,2)] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMongoTimestamp(0x0000000100000002L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestampInArrayStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : [{ $timestamp :{ t:1000,i:2 } } ] }");
        final DocumentBuilder b = BuilderFactory.start();
        b.pushArray("a").addMongoTimestamp(0x0000000100000002L);
        assertEquals(b.build(), doc);
    }

    /**
     * Test Parsing a Timestamp(..) element.
     * 
     * @throws ParseException
     *             On a test failure.
     */
    @Test
    public void testParseTimestampStrict() throws ParseException {
        final JsonParser parser = new JsonParser();

        final Object doc = parser
                .parse("{ a : { $timestamp : { t:1000,i:2 } } }");
        assertEquals(
                BuilderFactory.start()
                        .addMongoTimestamp("a", 0x0000000100000002L).build(),
                doc);
    }
}
