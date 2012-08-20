/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Date;
import java.util.regex.Pattern;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
        assertEquals(b.build().queryPath("f", "\\$add").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$and").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$cmp").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$cond").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$divide").get(0),
                e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$eq").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$gt").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$gte").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$ifNull").get(0),
                e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$lt").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$lte").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$mod").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$multiply").get(0),
                e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$ne").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$or").get(0), e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$strcasecmp").get(0),
                e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$substr").get(0),
                e.asElement());
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
        assertEquals(b.build().queryPath("f", "\\$subtract").get(0),
                e.asElement());
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
