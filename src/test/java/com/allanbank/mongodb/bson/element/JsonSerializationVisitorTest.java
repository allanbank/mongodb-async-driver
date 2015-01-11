/*
 * #%L
 * JsonSerializationVisitorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.bson.element;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

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
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * {@link JsonSerializationVisitor#visitObjectId(String, ObjectId)}.
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
     * {@link JsonSerializationVisitor#visitBinary(String, byte, byte[])}.
     */
    @Test
    public void testVisitStrictBinary() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        visitor.visitBinary("a", (byte) 0xF, new byte[] { (byte) 0xCA,
                (byte) 0xFE });

        assertThat(writer.toString(),
                is("\"a\" : { \"$binary\" : \"yv4=\", \"$type\" : \"f\" }"));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitDBPointer(String, String, String, ObjectId)}
     */
    @Test
    public void testVisitStrictDBPointer() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        final ObjectId id = new ObjectId();
        visitor.visitDBPointer("a", "b", "c", id);

        assertThat(
                writer.toString(),
                is("\"a\" : { \"$db\" : \"b\", \"$collection\" : \"c\", \"$id\" : { \"$oid\" : \""
                        + id.toHexString() + "\" } }"));
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitLong(String, long)}.
     */
    @Test
    public void testVisitStrictLong() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        visitor.visitLong("a", 1234567);

        assertThat(writer.toString(),
                is("\"a\" : { \"$numberLong\" : \"1234567\" }"));
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitMaxKey(String)}.
     */
    @Test
    public void testVisitStrictMaxKey() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        visitor.visitMaxKey("a");

        assertThat(writer.toString(), is("\"a\" : { \"$maxKey\" : 1 }"));
    }

    /**
     * Test method for {@link JsonSerializationVisitor#visitMinKey(String)}.
     */
    @Test
    public void testVisitStrictMinKey() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        visitor.visitMinKey("a");

        assertThat(writer.toString(), is("\"a\" : { \"$minKey\" : 1 }"));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitMongoTimestamp(String, long)}.
     */
    @Test
    public void testVisitStrictMongoTimestamp() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        visitor.visitMongoTimestamp("a", 123451234512345L);

        assertThat(
                writer.toString(),
                is("\"a\" : { \"$timestamp\" : { \"t\" : 28743000, \"i\" : 989523417 } }"));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitObjectId(String, ObjectId)}.
     */
    @Test
    public void testVisitStrictObjectId() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        final ObjectId id = new ObjectId();
        visitor.visitObjectId("a", id);

        assertThat(writer.toString(),
                is("\"a\" : { \"$oid\" : \"" + id.toHexString() + "\" }"));
    }

    /**
     * Test method for
     * {@link JsonSerializationVisitor#visitTimestamp(String, long)}.
     */
    @Test
    public void testVisitStrictTimestamp() {
        final StringWriter writer = new StringWriter();
        final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                writer, true, true);

        final SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        sdf.setTimeZone(JsonSerializationVisitor.UTC);
        final long now = System.currentTimeMillis();

        visitor.visitTimestamp("a", now);

        assertThat(writer.toString(),
                is("\"a\" : { \"$date\" : \"" + sdf.format(new Date(now))
                        + "\" }"));
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
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
