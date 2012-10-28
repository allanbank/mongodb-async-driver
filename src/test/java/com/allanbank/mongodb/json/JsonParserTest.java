/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.json;

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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * JsonParserTest provides tests for the {@link JsonParser}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
}
