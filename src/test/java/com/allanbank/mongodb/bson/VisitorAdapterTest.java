/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * VisitorAdapterTest provides tests for the {@link VisitorAdapter}.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VisitorAdapterTest {

    /**
     * Test method for {@link VisitorAdapter#visit(java.util.List)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testVisit() {
        DocumentBuilder builder = BuilderFactory.start();

        builder.addBoolean("true", true);
        final Document simple = builder.build();

        builder = BuilderFactory.start();
        builder.add(new BooleanElement("_id", false));
        builder.addBinary("binary", new byte[20]);
        builder.addBinary("binary-2", (byte) 2, new byte[40]);
        builder.addBoolean("true", true);
        builder.addBoolean("false", false);
        builder.addDBPointer("DBPointer", "db", "collection", new ObjectId(1,
                2L));
        builder.addDouble("double", 4884.45345);
        builder.addDocument("simple", simple);
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
        builder.addString("string", "string\u0090\ufffe");
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

        final Document doc = builder.build();

        final List<String> expected = Arrays.asList("_id", "binary",
                "binary-2", "true", "false", "DBPointer", "double", "simple",
                "true", "int", "javascript", "javascript_with_code", "true",
                "long", "max", "min", "mongo-time", "null", "object-id",
                "regex", "string", "symbol", "timestamp",
                // Sub-document with just a boolean.
                "sub-doc", "true", "array",
                // Array Elements.
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "true", "9", "10",
                "11", "12", "13", "14", "15", "16", "17", "18", "19",
                // Final document with just a boolean.
                "true");

        final List<String> names = new ArrayList<String>();
        final VisitorAdapter adapter = new VisitorAdapter() {
            @Override
            public void visitName(final String name) {
                super.visitName(name);
                names.add(name);
            }
        };

        doc.accept(adapter);

        assertThat(names, is(expected));
    }
}
