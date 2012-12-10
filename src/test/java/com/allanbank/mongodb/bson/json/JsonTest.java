/*
 * Copyright 2012, Allanbank Consulting, Inc. 
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

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * JsonTest provides TODO - Finish
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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

}
