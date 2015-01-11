/*
 * #%L
 * SizeOfVisitorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.io;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Date;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * SizeOfVisitorTest provides tests for the {@link SizeOfVisitor} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SizeOfVisitorTest {

    /**
     * Test for the visitor.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void test() {
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

        final SizeOfVisitor visitor = new SizeOfVisitor();

        doc.accept(visitor);
        assertThat(visitor.getSize(), is((int) doc.size()));

        doc.accept(visitor);
        assertThat(visitor.getSize(), is((int) doc.size() * 2));

        visitor.reset();
        assertThat(visitor.getSize(), is(0));

        doc.accept(visitor);
        assertThat(visitor.getSize(), is((int) doc.size()));

        visitor.rewind();
        assertThat(visitor.getSize(), is(0));
    }
}
