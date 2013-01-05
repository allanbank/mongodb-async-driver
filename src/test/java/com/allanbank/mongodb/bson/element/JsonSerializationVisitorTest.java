/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.error.JsonException;

/**
 * JsonSerializationVisitorTest provides tests for the
 * {@link JsonSerializationVisitor}. Most of the logic in the
 * {@link JsonSerializationVisitor} class is testing via the toString() tests
 * for each Element type. This test suite handles the error cases and corner
 * cases only.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JsonSerializationVisitorTest {

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitArray(String, java.util.List)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitArrayOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitArray("a", Collections
                .singletonList((Element) new BooleanElement("a", false)));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitBinary(String, byte, byte[])}.
     */
    @Test(expected = JsonException.class)
    public void testVisitBinaryOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitBinary("a", (byte) 0, new byte[1]);
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitBoolean(String, boolean)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitBooleanOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitBoolean("a", false);
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitDBPointer(String, String, String, ObjectId)}
     * .
     */
    @Test(expected = JsonException.class)
    public void testVisitDBPointerOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitDBPointer("a", "b", "c", new ObjectId());
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitDocument(String, java.util.List)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitDocumentOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitDocument("a", Collections
                .singletonList((Element) new BooleanElement("a", false)));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitDouble(String, double)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitDoubleOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitDouble("a", 123.456);
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitInteger(String, int)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitIntegerOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitInteger("a", 123);
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitJavaScript(String, String, Document)}
     * .
     */
    @Test(expected = JsonException.class)
    public void testVisitJavaScriptStringStringDocumentOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitJavaScript("a", "b", BuilderFactory.start().asDocument());
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitJavaScript(String, String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitJavaScriptStringStringOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitJavaScript("a", "b");
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitLong(String, long)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitLongOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitLong("a", 1234567);
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitMaxKey(String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitMaxKeyOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitMaxKey("a");
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitMinKey(String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitMinKeyOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitMinKey("a");
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitMongoTimestamp(String, long)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitMongoTimestampOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitMongoTimestamp("a", 12345);
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitNull(String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitNullOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitNull("a");
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitObjectId(String, ObjectId)} .
     */
    @Test(expected = JsonException.class)
    public void testVisitObjectIdOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitObjectId("a", new ObjectId());
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visit(java.util.List)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visit(Collections.singletonList((Element) new BooleanElement(
                "a", false)));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitRegularExpression(String, String, String)}
     * .
     */
    @Test(expected = JsonException.class)
    public void testVisitRegularExpressionOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitRegularExpression("a", "b", "c");
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitString(String, String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitStringOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitString("a", "b");
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitSymbol(String, String)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitSymbolOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitSymbol("a", "b");
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitSymbol(String, String)}.
     */
    @Test
    public void testVisitSymbolWithNonSymbol() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true);

        visitor.visitSymbol("a", "1234");

        assertEquals("a : '1234'", writer.toString());
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitTimestamp(String, long)}.
     */
    @Test(expected = JsonException.class)
    public void testVisitTimestampOnIOException() {
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                new ThrowingWriter(), true);

        visitor.visitTimestamp("a", 1234);
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#writeQuotedString(String)}.
     */
    @Test
    public void testWriteQuotedStringWithSingleQuote() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true);

        visitor.visitSymbol("a'b", "a'b\"c");

        assertEquals("\"a'b\" : 'a\\'b\"c'", writer.toString());
    }

    /**
     * ThrowingWriter provides a writer that throws an exception on all writes.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class ThrowingWriter extends Writer {

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            throw new IOException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void flush() throws IOException {
            throw new IOException();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void write(final char[] cbuf, final int off, final int len)
                throws IOException {
            throw new IOException();
        }
    }
}
