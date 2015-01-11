/*
 * #%L
 * ExpressionsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder.expression;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;

/**
 * ExpressionsTest provides tests for the {@link Expressions} class and
 * associated {@link Expression} implementations.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ExpressionsTest {

    /**
     * Test method for {@link Expressions#add(Expression[])}.
     */
    @Test
    public void testAdd() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));
        final Constant e3 = new Constant(new IntegerElement("", 3));

        final NaryExpression e = Expressions.add(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$add").addInteger(1).addInteger(2).addInteger(3);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$add").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#allElementsTrue(Expression)}.
     */
    @Test
    public void testAllElementsTrue() {

        final UnaryExpression e = Expressions.allElementsTrue(Expressions
                .constant(a(true, false)));

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray(Expressions.ALL_ELEMENTS_TRUE).add(true)
                .add(false);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\" + Expressions.ALL_ELEMENTS_TRUE)
                .get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#and(Expression[])}.
     */
    @Test
    public void testAnd() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));
        final Constant e3 = new Constant(new IntegerElement("", 3));

        final NaryExpression e = Expressions.and(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$and").addInteger(1).addInteger(2).addInteger(3);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$and").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#anyElementTrue(Expression)}.
     */
    @Test
    public void testAnyElementTrue() {

        final UnaryExpression e = Expressions.anyElementTrue(Expressions
                .constant(a(true, false)));

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray(Expressions.ANY_ELEMENT_TRUE).add(true)
                .add(false);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\" + Expressions.ANY_ELEMENT_TRUE)
                .get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#cmp(Expression, Expression)}.
     */
    @Test
    public void testCmp() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.cmp(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$cmp").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$cmp").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#concatenate(Expression[])}.
     */
    @Test
    public void testConcatenate() {
        final Constant e1 = new Constant(new StringElement("", "a"));
        final Constant e2 = new Constant(new StringElement("", "B"));
        final Constant e3 = new Constant(new StringElement("", "c"));

        final NaryExpression e = Expressions.concatenate(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$concat").add("a").add("B").add("c");
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$concat").get(0), e.asElement());
    }

    /**
     * Test method for
     * {@link Expressions#cond(Expression, Expression, Expression)}.
     */
    @Test
    public void testCond() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));
        final Constant e3 = new Constant(new IntegerElement("", 3));

        final NaryExpression e = Expressions.cond(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$cond").addInteger(1).addInteger(2)
                .addInteger(3);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$cond").get(0), e.asElement());

        assertEquals("'$cond' : [ 1,  2,  3 ]", e.toString());

    }

    /**
     * Test method for {@link Expressions#constant(boolean)}.
     */
    @Test
    public void testConstantBoolean() {
        assertEquals(new BooleanElement("f", true), Expressions.constant(true)
                .toElement("f"));
        assertEquals(new BooleanElement("f", false), Expressions
                .constant(false).toElement("f"));

        assertEquals("true", Expressions.constant(true).toString());
    }

    /**
     * Test method for {@link Expressions#constant(Date)}.
     */
    @Test
    public void testConstantDate() {
        final Date time = new Date();
        assertEquals(new TimestampElement("f", time.getTime()), Expressions
                .constant(time).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(double)}.
     */
    @Test
    public void testConstantDouble() {
        assertEquals(new DoubleElement("f", 1234), Expressions.constant(1234D)
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(Element)}.
     */
    @Test
    public void testConstantElement() {
        assertThat(Expressions.constant(BuilderFactory.a(true, false))
                .toElement("f"), is((Element) new ArrayElement("f",
                new BooleanElement("0", true), new BooleanElement("1", false))));
    }

    /**
     * Test method for {@link Expressions#constant(int)}.
     */
    @Test
    public void testConstantInt() {
        assertEquals(new IntegerElement("f", 1234), Expressions.constant(1234)
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(long)}.
     */
    @Test
    public void testConstantLong() {
        assertEquals(new LongElement("f", 1234), Expressions.constant(1234L)
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constantMongoTimestamp(long)}.
     */
    @Test
    public void testConstantMongoTimestamp() {
        final long time = System.currentTimeMillis();
        assertEquals(new MongoTimestampElement("f", time), Expressions
                .constantMongoTimestamp(time).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(ObjectId)} .
     */
    @Test
    public void testConstantObjectId() {
        final ObjectId id = new ObjectId();
        assertEquals(new ObjectIdElement("f", id), Expressions.constant(id)
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(Pattern)}.
     */
    @Test
    public void testConstantPattern() {
        final Pattern pattern = Pattern.compile("abc");
        assertEquals(new RegularExpressionElement("f", pattern), Expressions
                .constant(pattern).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constant(String)}.
     */
    @Test
    public void testConstantString() {
        assertEquals(new StringElement("f", "s"), Expressions.constant("s")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#constantTimestamp(long)}.
     */
    @Test
    public void testConstantTimestamp() {
        final long time = System.currentTimeMillis();
        assertEquals(new TimestampElement("f", time), Expressions
                .constantTimestamp(time).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#dateToString(String, Expression)}.
     */
    @Test
    public void testDateToString() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final NamedNaryExpression e = Expressions.dateToString("%Y", e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").push("$dateToString").add("format", "%Y")
                .addTimestamp("date", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#dayOfMonth(Expression)}.
     */
    @Test
    public void testDayOfMonth() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.dayOfMonth(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$dayOfMonth", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#dayOfWeek(Expression)}.
     */
    @Test
    public void testDayOfWeek() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.dayOfWeek(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$dayOfWeek", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#dayOfYear(Expression)}.
     */
    @Test
    public void testDayOfYear() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.dayOfYear(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$dayOfYear", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));

        assertEquals("'$dayOfYear' : ISODate('1970-01-01T00:00:00.001+0000')",
                e.toString());
    }

    /**
     * Test method for {@link Expressions#divide(Expression, Expression)}.
     */
    @Test
    public void testDivide() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.divide(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$divide").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$divide").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#eq(Expression, Expression)}.
     */
    @Test
    public void testEq() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.eq(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$eq").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$eq").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#field(String)}.
     */
    @Test
    public void testFieldWithDollar() {
        assertEquals(new StringElement("f", "$foo"), Expressions.field("$foo")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#field(String)}.
     */
    @Test
    public void testFieldWithoutDollar() {
        assertEquals(new StringElement("f", "$foo"), Expressions.field("foo")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#gt(Expression, Expression)}.
     */
    @Test
    public void testGt() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.gt(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$gt").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$gt").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#gte(Expression, Expression)}.
     */
    @Test
    public void testGte() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.gte(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$gte").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$gte").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#hour(Expression)}.
     */
    @Test
    public void testHour() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.hour(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$hour", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#ifNull(Expression, Expression)}.
     */
    @Test
    public void testIfNull() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.ifNull(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$ifNull").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$ifNull").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#let(Expression, Element...)}.
     */
    @Test
    public void testLetExpressionElelements() {
        assertEquals(
                new DocumentElement("f",
                        d(
                                e("$let",
                                        d(e("vars", d(e("foo", 1), e("bar", 2))
                                                .build()), e("in", 3))))
                                .build()),
                Expressions.let(Expressions.constant(3),
                        Expressions.set("foo", Expressions.constant(1)),
                        Expressions.set("bar", Expressions.constant(2)))
                        .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#let(java.util.List,Expression)}.
     */
    @Test
    public void testLetListElementsExpression() {
        assertEquals(
                new DocumentElement("f",
                        d(
                                e("$let",
                                        d(e("vars", d(e("foo", 1), e("bar", 2))
                                                .build()), e("in", 3))))
                                .build()),
                Expressions
                        .let(Arrays
                                .asList(Expressions.set("foo",
                                        Expressions.constant(1)),
                                        Expressions.set("bar",
                                                Expressions.constant(2))),
                                Expressions.constant(3)).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#let(String, Expression)}.
     */
    @Test
    public void testLetMultipleVars() {
        assertEquals(
                new DocumentElement("f", d(
                        e("$let",
                                d(e("vars",
                                        d(
                                                e("foo", 1),
                                                e("bar", 2),
                                                e("baz", d(e("$literal", 1))
                                                        .build())).build()),
                                        e("in", 3)))).build()),
                Expressions.let("foo", Expressions.constant(1))
                        .let("bar", Expressions.constant(2))
                        .let("baz", BuilderFactory.start().add("$literal", 1))
                        .in(Expressions.constant(3)).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#let(String, DocumentAssignable)}.
     */
    @Test
    public void testLetStringDocumentAssignable() {
        assertEquals(
                new DocumentElement(
                        "f",
                        d(
                                e("$let",
                                        d(e("vars",
                                                d(
                                                        e("foo",
                                                                d(
                                                                        e("$literal",
                                                                                1))
                                                                        .build()))
                                                        .build()), e("in", 3))))
                                .build()),
                Expressions
                        .let("foo", BuilderFactory.start().add("$literal", 1))
                        .in(Expressions.constant(3)).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#let(String, Expression)}.
     */
    @Test
    public void testLetStringExpression() {
        assertEquals(
                new DocumentElement("f", BuilderFactory.d(
                        BuilderFactory.e("$let", BuilderFactory.d(
                                BuilderFactory.e(
                                        "vars",
                                        BuilderFactory.d(
                                                BuilderFactory.e("foo", 1))
                                                .build()), BuilderFactory.e(
                                        "in", 3)))).build()),
                Expressions.let("foo", Expressions.constant(1))
                        .in(Expressions.constant(3)).toElement("f"));
    }

    /**
     * Test method for {@link Expressions#literal(String)}.
     */
    @Test
    public void testLiteral() {

        final Constant e = Expressions.literal("$foo");

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").add(Expressions.LITERAL, "$foo");
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#lt(Expression, Expression)}.
     */
    @Test
    public void testLt() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.lt(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$lt").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$lt").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#lte(Expression, Expression)}.
     */
    @Test
    public void testLte() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.lte(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$lte").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$lte").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#map(String)}.
     */
    @Test
    public void testMapString() {
        assertEquals(
                new DocumentElement("f", BuilderFactory.d(
                        BuilderFactory.e("$map", BuilderFactory.d(
                                BuilderFactory.e("input", "$foo"),
                                BuilderFactory.e("as", "bar"),
                                BuilderFactory.e("in", 3)))).build()),
                Expressions.map("foo").as("bar").in(Expressions.constant(3))
                        .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#map(String,String,Expression)}.
     */
    @Test
    public void testMapStringStringExpression() {
        assertEquals(
                new DocumentElement("f", BuilderFactory.d(
                        BuilderFactory.e("$map", BuilderFactory.d(
                                BuilderFactory.e("input", "$foo"),
                                BuilderFactory.e("as", "bar"),
                                BuilderFactory.e("in", 3)))).build()),
                Expressions.map("foo", "bar", Expressions.constant(3))
                        .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#millisecond(Expression)}.
     */
    @Test
    public void testMillisecond() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.millisecond(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$millisecond", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#minute(Expression)}.
     */
    @Test
    public void testMinute() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.minute(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$minute", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#mod(Expression, Expression)}.
     */
    @Test
    public void testMod() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.mod(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$mod").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$mod").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#month(Expression)}.
     */
    @Test
    public void testMonth() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.month(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$month", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#multiply(Expression, Expression)}.
     */
    @Test
    public void testMultiply() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.multiply(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$multiply").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$multiply").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#ne(Expression, Expression)}.
     */
    @Test
    public void testNe() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.ne(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$ne").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$ne").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#not(Expression)}.
     */
    @Test
    public void testNot() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.not(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$not", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#nullConstant()}.
     */
    @Test
    public void testNullConstant() {
        assertEquals(new NullElement("f"), Expressions.nullConstant()
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#or(Expression[])}.
     */
    @Test
    public void testOr() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));
        final Constant e3 = new Constant(new IntegerElement("", 3));

        final NaryExpression e = Expressions.or(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$or").addInteger(1).addInteger(2).addInteger(3);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$or").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#second(Expression)}.
     */
    @Test
    public void testSecond() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.second(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$second", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#setDifference(Expression, Expression)}
     * .
     */
    @Test
    public void testSetDifference() {

        final Constant e1 = new Constant(a("a", "b"));
        final Constant e2 = new Constant(a("a", "b", "c"));

        final NaryExpression e = Expressions.setDifference(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f").pushArray(
                Expressions.SET_DIFFERENCE);
        arrays.pushArray().add("a").add("b");
        arrays.pushArray().add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#setEquals(Expression, Expression)} .
     */
    @Test
    public void testSetEquals() {

        final Constant e1 = new Constant(a("a", "b"));
        final Constant e2 = new Constant(a("a", "b", "c"));

        final NaryExpression e = Expressions.setEquals(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f").pushArray(
                Expressions.SET_EQUALS);
        arrays.pushArray().add("a").add("b");
        arrays.pushArray().add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for
     * {@link Expressions#setIntersection(Expression, Expression)} .
     */
    @Test
    public void testSetIntersection() {

        final Constant e1 = new Constant(a("a", "b"));
        final Constant e2 = new Constant(a("a", "b", "c"));

        final NaryExpression e = Expressions.setIntersection(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f").pushArray(
                Expressions.SET_INTERSECTION);
        arrays.pushArray().add("a").add("b");
        arrays.pushArray().add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#setIsSubset(Expression, Expression)} .
     */
    @Test
    public void testSetIsSubset() {

        final Constant e1 = new Constant(a("a", "b"));
        final Constant e2 = new Constant(a("a", "b", "c"));

        final NaryExpression e = Expressions.setIsSubset(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f").pushArray(
                Expressions.SET_IS_SUBSET);
        arrays.pushArray().add("a").add("b");
        arrays.pushArray().add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#set(String, DocumentAssignable)} .
     */
    @Test
    public void testSetStringDocumentAssignable() {
        final DocumentBuilder b = BuilderFactory.start();
        b.addInteger("f", 1);

        final Element e = Expressions.set("f", b);

        assertNotNull(e);

        assertEquals(new DocumentElement("f", b.build()), e);
    }

    /**
     * Test method for {@link Expressions#set(String, Expression)}.
     */
    @Test
    public void testSetStringExpression() {

        final Constant e1 = new Constant(new TimestampElement("", 1));

        final Element e = Expressions.set("f", Expressions.second(e1));

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$second", 1);
        assertEquals(b.build().iterator().next(), e);
    }

    /**
     * Test method for {@link Expressions#setUnion(Expression, Expression)} .
     */
    @Test
    public void testSetUnion() {

        final Constant e1 = new Constant(a("a", "b"));
        final Constant e2 = new Constant(a("a", "b", "c"));

        final NaryExpression e = Expressions.setUnion(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f")
                .pushArray(Expressions.SET_UNION);
        arrays.pushArray().add("a").add("b");
        arrays.pushArray().add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#size(Expression)} .
     */
    @Test
    public void testSize() {

        final Constant e2 = new Constant(a("a", "b", "c"));

        final UnaryExpression e = Expressions.size(e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        final ArrayBuilder arrays = b.push("f").pushArray(Expressions.SIZE);
        arrays.add("a").add("b").add("c");

        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#strcasecmp(Expression, Expression)}.
     */
    @Test
    public void testStrcasecmp() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.strcasecmp(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$strcasecmp").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$strcasecmp").get(0), e.asElement());
    }

    /**
     * Test method for
     * {@link Expressions#substr(Expression, Expression, Expression)}.
     */
    @Test
    public void testSubstr() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));
        final Constant e3 = new Constant(new IntegerElement("", 3));

        final NaryExpression e = Expressions.substr(e1, e2, e3);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$substr").addInteger(1).addInteger(2)
                .addInteger(3);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$substr").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#subtract(Expression, Expression)}.
     */
    @Test
    public void testSubtract() {
        final Constant e1 = new Constant(new IntegerElement("", 1));
        final Constant e2 = new Constant(new IntegerElement("", 2));

        final NaryExpression e = Expressions.subtract(e1, e2);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").pushArray("$subtract").addInteger(1).addInteger(2);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(b.build().find("f", "\\$subtract").get(0), e.asElement());
    }

    /**
     * Test method for {@link Expressions#toLower(Expression)}.
     */
    @Test
    public void testToLower() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.toLower(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$toLower", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#toUpper(Expression)}.
     */
    @Test
    public void testToUpper() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.toUpper(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$toUpper", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }

    /**
     * Test method for {@link Expressions#var(String)}.
     */
    @Test
    public void testVarWithOneDollar() {
        assertEquals(new StringElement("f", "$$foo"), Expressions.var("$foo")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#var(String)}.
     */
    @Test
    public void testVarWithoutDollar() {
        assertEquals(new StringElement("f", "$$foo"), Expressions.var("foo")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#var(String)}.
     */
    @Test
    public void testVarWithTwoDollars() {
        assertEquals(new StringElement("f", "$$foo"), Expressions.var("$$foo")
                .toElement("f"));
    }

    /**
     * Test method for {@link Expressions#week(Expression)}.
     */
    @Test
    public void testWeek() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.week(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$week", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
        assertEquals(new TimestampElement("$week", 1), e.asElement());
    }

    /**
     * Test method for {@link Expressions#year(Expression)}.
     */
    @Test
    public void testYear() {
        final Constant e1 = new Constant(new TimestampElement("", 1));

        final UnaryExpression e = Expressions.year(e1);

        assertNotNull(e);

        final DocumentBuilder b = BuilderFactory.start();
        b.push("f").addTimestamp("$year", 1);
        assertEquals(b.build().iterator().next(), e.toElement("f"));
    }
}
