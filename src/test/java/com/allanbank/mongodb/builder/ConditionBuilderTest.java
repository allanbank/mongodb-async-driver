/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.JavaScriptElement;
import com.allanbank.mongodb.bson.element.JavaScriptWithScopeElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MaxKeyElement;
import com.allanbank.mongodb.bson.element.MinKeyElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.NullElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.element.UuidElement;
import com.allanbank.mongodb.builder.expression.Expressions;

/**
 * ConditionBuilderTest provides tests for the {@link ConditionBuilder} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConditionBuilderTest {

    /** Source of randomness in the tests. */
    private Random myRandom;

    /**
     * Initialize the test state.
     */
    @Before
    public void setUp() {
        myRandom = new Random(System.currentTimeMillis());
    }

    /**
     * Cleanup from the test.
     */
    @After
    public void tearDown() {
        myRandom = null;
    }

    /**
     * Test method for
     * {@link ConditionBuilder#all(com.allanbank.mongodb.bson.builder.ArrayBuilder)}
     * .
     */
    @Test
    public void testAllArrayBuilder() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.addBoolean(false).addBoolean(true);

        b.equals(false); // Make sure equals is removed.
        b.all(ab);

        // Should not change the results.
        ab.addInteger(1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.ALL.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#all}.
     */
    @Test
    public void testAllConstantArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.all(Expressions.constant(false), Expressions.constant(true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.ALL.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#all(com.allanbank.mongodb.bson.Element[])}.
     */
    @Test
    public void testAllElementArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.all(new BooleanElement("0", false), new BooleanElement("1", true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.ALL.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#and(String)}.
     */
    @Test
    public void testAndDifferentField() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.addBoolean(false).addBoolean(true);

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(12).and("bar").lessThan(23);

        // Should not change the results.
        ab.addInteger(1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.GT.getToken(), 12);

        assertEquals(e, new DocumentElement("foo", db.build()));

        db = BuilderFactory.start();
        db.push("foo").addInteger(ComparisonOperator.GT.getToken(), 12);
        db.push("bar").addInteger(ComparisonOperator.LT.getToken(), 23);

        assertEquals(db.build(), b.build());
    }

    /**
     * Test method for {@link ConditionBuilder#and(String)}.
     */
    @Test
    public void testAndSameField() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.addBoolean(false).addBoolean(true);

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(12).and("foo").lessThan(23);

        // Should not change the results.
        ab.addInteger(1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.GT.getToken(), 12);
        db.addInteger(ComparisonOperator.LT.getToken(), 23);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#elementMatches(DocumentAssignable)} .
     */
    @Test
    public void testElementMatchesDocument() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final DocumentAssignable value = BuilderFactory.start().addInteger(
                String.valueOf(myRandom.nextInt()), myRandom.nextInt());

        b.equals(false); // Make sure equals is removed.
        b.elementMatches(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDocument(MiscellaneousOperator.ELEMENT_MATCH.getToken(),
                value.asDocument());

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#elementMatches} .
     */
    @Test
    public void testElementMatchesQueryBuilder() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final QueryBuilder qb2 = new QueryBuilder();
        qb2.whereField("f").equals(123);

        b.equals(false); // Make sure equals is removed.
        b.elementMatches(qb2);

        final Document value = qb2.build();

        // Should not effect the value.
        qb2.whereField("f").equals(true);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDocument(MiscellaneousOperator.ELEMENT_MATCH.getToken(), value);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(boolean)}.
     */
    @Test
    public void testEqualsBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final boolean value = myRandom.nextBoolean();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(BooleanElement.class));
        assertEquals(e, new BooleanElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(byte[])}.
     */
    @Test
    public void testEqualsByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(BinaryElement.class));
        assertEquals(e, new BinaryElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(byte, byte[])}.
     */
    @Test
    public void testEqualsByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(BinaryElement.class));
        assertEquals(e, new BinaryElement("foo", type, value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(DocumentAssignable)}.
     */
    @Test
    public void testEqualsDocument() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final Document value = BuilderFactory
                .start()
                .addInteger(String.valueOf(myRandom.nextInt()),
                        myRandom.nextInt()).build();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));
        assertEquals(e, new DocumentElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(double)}.
     */
    @Test
    public void testEqualsDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DoubleElement.class));
        assertEquals(e, new DoubleElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(int)}.
     */
    @Test
    public void testEqualsInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(IntegerElement.class));
        assertEquals(e, new IntegerElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsJavaScript(String)}.
     */
    @Test
    public void testEqualsJavaScriptString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsJavaScript(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(JavaScriptElement.class));
        assertEquals(e, new JavaScriptElement("foo", value));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#equalsJavaScript(String, DocumentAssignable)} .
     */
    @Test
    public void testEqualsJavaScriptStringDocument() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());
        final Document scope = BuilderFactory
                .start()
                .addInteger(String.valueOf(myRandom.nextInt()),
                        myRandom.nextInt()).build();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsJavaScript(value, scope);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(JavaScriptWithScopeElement.class));
        assertEquals(e, new JavaScriptWithScopeElement("foo", value, scope));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsLegacy(UUID)}.
     */
    @Test
    public void testEqualsLegacyUUID() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final UUID value = UUID.randomUUID();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsLegacy(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(UuidElement.class));
        assertEquals(e, new UuidElement("foo",
                UuidElement.LEGACY_UUID_SUBTTYPE, value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(long)}.
     */
    @Test
    public void testEqualsLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(LongElement.class));
        assertEquals(e, new LongElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMaxKey()}.
     */
    @Test
    public void testEqualsMaxKey() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsMaxKey();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(MaxKeyElement.class));
        assertEquals(e, new MaxKeyElement("foo"));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMinKey()}.
     */
    @Test
    public void testEqualsMinKey() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsMinKey();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(MinKeyElement.class));
        assertEquals(e, new MinKeyElement("foo"));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMongoTimestamp(long)}.
     */
    @Test
    public void testEqualsMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(MongoTimestampElement.class));
        assertEquals(e, new MongoTimestampElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsNull()}.
     */
    @Test
    public void testEqualsNull() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsNull();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(NullElement.class));
        assertEquals(e, new NullElement("foo"));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(ObjectId)} .
     */
    @Test
    public void testEqualsObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(ObjectIdElement.class));
        assertEquals(e, new ObjectIdElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(java.util.regex.Pattern)}.
     */
    @Test
    public void testEqualsPattern() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final Pattern value = Pattern.compile(".*");

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(RegularExpressionElement.class));
        assertEquals(e, new RegularExpressionElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(String)}.
     */
    @Test
    public void testEqualsString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(StringElement.class));
        assertEquals(e, new StringElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsSymbol(String)}.
     */
    @Test
    public void testEqualsSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(SymbolElement.class));
        assertEquals(e, new SymbolElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equalsTimestamp(long)}.
     */
    @Test
    public void testEqualsTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equalsTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(TimestampElement.class));
        assertEquals(e, new TimestampElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#equals(UUID)}.
     */
    @Test
    public void testEqualsUUID() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final UUID value = UUID.randomUUID();

        b.greaterThan(23); // Make sure non-equals is removed.
        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(UuidElement.class));
        assertEquals(e, new UuidElement("foo", value));
    }

    /**
     * Test method for {@link ConditionBuilder#exists()}.
     */
    @Test
    public void testExists() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final boolean value = true;

        b.equals(false); // Make sure equals is cleared
        b.exists();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBoolean(MiscellaneousOperator.EXISTS.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#exists(boolean)}.
     */
    @Test
    public void testExistsBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final boolean value = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.exists(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBoolean(MiscellaneousOperator.EXISTS.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#geoWithin(DocumentAssignable)}.
     */
    @Test
    public void testGeoWithin() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 + myRandom.nextInt(1024);
        final double y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.geoWithin(GeoJson.polygon(Arrays.asList(GeoJson.p(x1, y1),
                GeoJson.p(x1, y2), GeoJson.p(x2, y2), GeoJson.p(x2, y1),
                GeoJson.p(x1, y1))));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder ib = db.push(GeospatialOperator.GEO_WITHIN
                .getToken());
        ib.add(GeospatialOperator.GEOMETRY, GeoJson.polygon(Arrays.asList(
                GeoJson.p(x1, y1), GeoJson.p(x1, y2), GeoJson.p(x2, y2),
                GeoJson.p(x2, y1), GeoJson.p(x1, y1))));

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#getFieldName()}.
     */
    @Test
    public void testGetFieldName() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        assertEquals("foo", b.getFieldName());
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(byte[])}.
     */
    @Test
    public void testGreaterThanByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(byte, byte[])}.
     */
    @Test
    public void testGreaterThanByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.GT.getToken(), type, value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(double)}.
     */
    @Test
    public void testGreaterThanDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDouble(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(int)}.
     */
    @Test
    public void testGreaterThanInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(long)}.
     */
    @Test
    public void testGreaterThanLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLong(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanMongoTimestamp(long)}.
     */
    @Test
    public void testGreaterThanMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMongoTimestamp(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(ObjectId)} .
     */
    @Test
    public void testGreaterThanObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addObjectId(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(byte[])}.
     */
    @Test
    public void testGreaterThanOrEqualsByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualTo(byte, byte[])}.
     */
    @Test
    public void testGreaterThanOrEqualsByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.GTE.getToken(), type, value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(double)}.
     */
    @Test
    public void testGreaterThanOrEqualsDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDouble(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(int)}.
     */
    @Test
    public void testGreaterThanOrEqualsInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(long)}.
     */
    @Test
    public void testGreaterThanOrEqualsLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLong(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testGreaterThanOrEqualsMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualToMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMongoTimestamp(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(ObjectId)} .
     */
    @Test
    public void testGreaterThanOrEqualsObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addObjectId(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(String)}.
     */
    @Test
    public void testGreaterThanOrEqualsString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addString(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToSymbol(String)}.
     */
    @Test
    public void testGreaterThanOrEqualsSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualToSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addSymbol(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToTimestamp(long)}.
     */
    @Test
    public void testGreaterThanOrEqualsTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanOrEqualToTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addTimestamp(ComparisonOperator.GTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(String)}.
     */
    @Test
    public void testGreaterThanString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.greaterThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addString(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanSymbol(String)}.
     */
    @Test
    public void testGreaterThanSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.greaterThanSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addSymbol(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanTimestamp(long)}.
     */
    @Test
    public void testGreaterThanTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.greaterThanTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addTimestamp(ComparisonOperator.GT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#in(com.allanbank.mongodb.bson.builder.ArrayBuilder)}
     * .
     */
    @Test
    public void testInArrayBuilder() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.addBoolean(false).addBoolean(true);

        b.equals(false); // Make sure equals is removed.
        b.in(ab);

        // Should not change the results.
        ab.addInteger(1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.IN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#in}.
     */
    @Test
    public void testInConstantArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.in(Expressions.constant(false), Expressions.constant(true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.IN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#in(com.allanbank.mongodb.bson.Element[])}.
     */
    @Test
    public void testInElementArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.in(new BooleanElement("0", false), new BooleanElement("1", true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.IN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#instanceOf(com.allanbank.mongodb.bson.ElementType)}
     * .
     */
    @Test
    public void testInstanceOf() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ElementType value = ElementType.values()[myRandom
                .nextInt(ElementType.values().length)];

        b.equals(false); // Make sure equals is removed.
        b.instanceOf(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(MiscellaneousOperator.TYPE.getToken(), value.getToken());

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#intersects(DocumentAssignable)}.
     */
    @Test
    public void testIntersects() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 + myRandom.nextInt(1024);
        final double y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.intersects(GeoJson.polygon(Arrays.asList(GeoJson.p(x1, y1),
                GeoJson.p(x1, y2), GeoJson.p(x2, y2), GeoJson.p(x2, y1),
                GeoJson.p(x1, y1))));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder ib = db.push(GeospatialOperator.INTERSECT
                .getToken());
        ib.add(GeospatialOperator.GEOMETRY, GeoJson.polygon(Arrays.asList(
                GeoJson.p(x1, y1), GeoJson.p(x1, y2), GeoJson.p(x2, y2),
                GeoJson.p(x2, y1), GeoJson.p(x1, y1))));

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(byte[])}.
     */
    @Test
    public void testLessThanByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(byte, byte[])}.
     */
    @Test
    public void testLessThanByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.lessThan(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.LT.getToken(), type, value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(double)}.
     */
    @Test
    public void testLessThanDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDouble(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(int)}.
     */
    @Test
    public void testLessThanInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(long)}.
     */
    @Test
    public void testLessThanLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLong(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanMongoTimestamp(long)}.
     */
    @Test
    public void testLessThanMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThanMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMongoTimestamp(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(ObjectId)} .
     */
    @Test
    public void testLessThanObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addObjectId(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(byte[])}.
     */
    @Test
    public void testLessThanOrEqualsByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(byte, byte[])}.
     */
    @Test
    public void testLessThanOrEqualsByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.LTE.getToken(), type, value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(double)}.
     */
    @Test
    public void testLessThanOrEqualsDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDouble(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(int)}.
     */
    @Test
    public void testLessThanOrEqualsInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(long)}.
     */
    @Test
    public void testLessThanOrEqualsLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLong(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#lessThanOrEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testLessThanOrEqualsMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualToMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMongoTimestamp(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(ObjectId)} .
     */
    @Test
    public void testLessThanOrEqualsObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addObjectId(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(String)}.
     */
    @Test
    public void testLessThanOrEqualsString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addString(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualToSymbol(String)}.
     */
    @Test
    public void testLessThanOrEqualsSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualToSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addSymbol(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualToTimestamp(long)}
     * .
     */
    @Test
    public void testLessThanOrEqualsTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThanOrEqualToTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addTimestamp(ComparisonOperator.LTE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(String)}.
     */
    @Test
    public void testLessThanString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.lessThan(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addString(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanSymbol(String)}.
     */
    @Test
    public void testLessThanSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.lessThanSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addSymbol(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanTimestamp(long)}.
     */
    @Test
    public void testLessThanTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.lessThanTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addTimestamp(ComparisonOperator.LT.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#matches(java.util.regex.Pattern)}
     * .
     */
    @Test
    public void testMatches() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final Pattern value = Pattern.compile(".*");

        b.equals(false); // Make sure equals is removed.
        b.matches(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addRegularExpression(MiscellaneousOperator.REG_EX.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#mod(int, int)}.
     */
    @Test
    public void testModIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int v1 = myRandom.nextInt(1024);
        final int v2 = myRandom.nextInt(v1);

        b.equals(false); // Make sure equals is removed.
        b.mod(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.MOD.getToken()).addInteger(v1)
                .addInteger(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#mod(long, long)}.
     */
    @Test
    public void testModLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.mod(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.MOD.getToken()).addLong(v1)
                .addLong(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(DocumentAssignable)}.
     */
    @Test
    public void testNearDocumentAssignable() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.near(GeoJson.point(GeoJson.p(v1, v2)));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.push(GeospatialOperator.NEAR.getToken()).add(
                GeospatialOperator.GEOMETRY, GeoJson.point(GeoJson.p(v1, v2)));

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(DocumentAssignable,double)}.
     */
    @Test
    public void testNearDocumentAssignableDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.near(GeoJson.point(GeoJson.p(v1, v2)), 42.1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.push(GeospatialOperator.NEAR.getToken())
                .add(GeospatialOperator.GEOMETRY,
                        GeoJson.point(GeoJson.p(v1, v2)))
                .add(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), 42.1);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double)}.
     */
    @Test
    public void testNearDoubleDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addDouble(v1)
                .addDouble(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double, double)}.
     */
    @Test
    public void testNearDoubleDoubleDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();
        final double v3 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addDouble(v1)
                .addDouble(v2);
        db.addDouble(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);
        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int)}.
     */
    @Test
    public void testNearIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addInteger(v1)
                .addInteger(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int, int)}.
     */
    @Test
    public void testNearIntIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();
        final int v3 = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addInteger(v1)
                .addInteger(v2);
        db.addInteger(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long)}.
     */
    @Test
    public void testNearLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addLong(v1)
                .addLong(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long, long)}.
     */
    @Test
    public void testNearLongLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();
        final long v3 = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.near(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR.getToken()).addLong(v1)
                .addLong(v2);
        db.addLong(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(DocumentAssignable)}.
     */
    @Test
    public void testNearSphereDocumentAssignable() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(GeoJson.point(GeoJson.p(v1, v2)));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.push(GeospatialOperator.NEAR_SPHERE.getToken()).add(
                GeospatialOperator.GEOMETRY, GeoJson.point(GeoJson.p(v1, v2)));

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#nearSphere(DocumentAssignable,double)}.
     */
    @Test
    public void testNearSphereDocumentAssignableDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(GeoJson.point(GeoJson.p(v1, v2)), 42.1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.push(GeospatialOperator.NEAR_SPHERE.getToken())
                .add(GeospatialOperator.GEOMETRY,
                        GeoJson.point(GeoJson.p(v1, v2)))
                .add(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), 42.1);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(double, double)}.
     */
    @Test
    public void testNearSphereDoubleDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addDouble(v1)
                .addDouble(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#nearSphere(double, double, double)}.
     */
    @Test
    public void testNearSphereDoubleDoubleDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double v1 = myRandom.nextDouble();
        final double v2 = myRandom.nextDouble();
        final double v3 = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addDouble(v1)
                .addDouble(v2);
        db.addDouble(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int)}.
     */
    @Test
    public void testNearSphereIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addInteger(v1)
                .addInteger(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int, int)}.
     */
    @Test
    public void testNearSphereIntIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();
        final int v3 = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addInteger(v1)
                .addInteger(v2);
        db.addInteger(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long)}.
     */
    @Test
    public void testNearSphereLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addLong(v1)
                .addLong(v2);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long, long)}.
     */
    @Test
    public void testNearSphereLongLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();
        final long v3 = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.nearSphere(v1, v2, v3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(GeospatialOperator.NEAR_SPHERE.getToken()).addLong(v1)
                .addLong(v2);
        db.addLong(GeospatialOperator.MAX_DISTANCE_MODIFIER.getToken(), v3);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(boolean)}.
     */
    @Test
    public void testNotEqualToBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final boolean value = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBoolean(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(byte[])}.
     */
    @Test
    public void testNotEqualToByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(byte, byte[])}.
     */
    @Test
    public void testNotEqualToByteByteArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final byte type = (byte) myRandom.nextInt();
        final byte[] value = new byte[myRandom.nextInt(1024)];
        myRandom.nextBytes(value);

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(type, value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addBinary(ComparisonOperator.NE.getToken(), type, value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(DocumentAssignable)}.
     */
    @Test
    public void testNotEqualToDocument() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final Document value = BuilderFactory
                .start()
                .addInteger(String.valueOf(myRandom.nextInt()),
                        myRandom.nextInt()).build();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDocument(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(double)}.
     */
    @Test
    public void testNotEqualToDouble() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double value = myRandom.nextDouble();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addDouble(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(int)}.
     */
    @Test
    public void testNotEqualToInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToJavaScript(String)}.
     */
    @Test
    public void testNotEqualToJavaScriptString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.notEqualToJavaScript(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addJavaScript(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notEqualToJavaScript(String, DocumentAssignable)}
     * .
     */
    @Test
    public void testNotEqualToJavaScriptStringDocument() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());
        final Document scope = BuilderFactory
                .start()
                .addInteger(String.valueOf(myRandom.nextInt()),
                        myRandom.nextInt()).build();

        b.equals(false); // Make sure equals is removed.
        b.notEqualToJavaScript(value, scope);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addJavaScript(ComparisonOperator.NE.getToken(), value, scope);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(UUID)} .
     */
    @Test
    public void testNotEqualToLegacyUUID() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final UUID value = UUID.randomUUID();

        b.equals(false); // Make sure equals is removed.
        b.notEqualToLegacy(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLegacyUuid(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(long)}.
     */
    @Test
    public void testNotEqualToLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addLong(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMaxKey()}.
     */
    @Test
    public void testNotEqualToMaxKey() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.notEqualToMaxKey();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMaxKey(ComparisonOperator.NE.getToken());

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMinKey()}.
     */
    @Test
    public void testNotEqualToMinKey() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.notEqualToMinKey();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMinKey(ComparisonOperator.NE.getToken());

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testNotEqualToMongoTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.notEqualToMongoTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addMongoTimestamp(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToNull()}.
     */
    @Test
    public void testNotEqualToNull() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.notEqualToNull();

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addNull(ComparisonOperator.NE.getToken());

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(ObjectId)} .
     */
    @Test
    public void testNotEqualToObjectId() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ObjectId value = new ObjectId();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addObjectId(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notEqualTo(java.util.regex.Pattern)}.
     */
    @Test
    public void testNotEqualToPattern() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final Pattern value = Pattern.compile(".*");

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addRegularExpression(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(String)}.
     */
    @Test
    public void testNotEqualToString() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addString(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToSymbol(String)}.
     */
    @Test
    public void testNotEqualToSymbol() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final String value = String.valueOf(myRandom.nextFloat());

        b.equals(false); // Make sure equals is removed.
        b.notEqualToSymbol(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addSymbol(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToTimestamp(long)}.
     */
    @Test
    public void testNotEqualToTimestamp() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long value = myRandom.nextLong();

        b.equals(false); // Make sure equals is removed.
        b.notEqualToTimestamp(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addTimestamp(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(UUID)} .
     */
    @Test
    public void testNotEqualToUUID() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final UUID value = UUID.randomUUID();

        b.equals(false); // Make sure equals is removed.
        b.notEqualTo(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.add(ComparisonOperator.NE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notIn(com.allanbank.mongodb.bson.builder.ArrayBuilder)}
     * .
     */
    @Test
    public void testNotInArrayBuilder() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final ArrayBuilder ab = BuilderFactory.startArray();
        ab.addBoolean(false).addBoolean(true);

        b.equals(false); // Make sure equals is removed.
        b.notIn(ab);

        // Should not change the results.
        ab.addInteger(1);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.NIN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notIn(com.allanbank.mongodb.bson.Element[])}.
     */
    @Test
    public void testNotInConstantArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.notIn(Expressions.constant(false), Expressions.constant(true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.NIN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notIn(com.allanbank.mongodb.bson.Element[])}.
     */
    @Test
    public void testNotInElementArray() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is removed.
        b.notIn(new BooleanElement("0", false), new BooleanElement("1", true));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.pushArray(MiscellaneousOperator.NIN.getToken()).addBoolean(false)
                .addBoolean(true);

        assertEquals(e, new DocumentElement("foo", db.build()));
    }

    /**
     * Test method for {@link ConditionBuilder#reset()}.
     */
    @Test
    public void testReset() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final boolean value = myRandom.nextBoolean();

        b.equals(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(BooleanElement.class));
        assertEquals(e, new BooleanElement("foo", value));

        b.reset();

        assertNull(b.buildFieldCondition());
    }

    /**
     * Test method for {@link ConditionBuilder#size(int)}.
     */
    @Test
    public void testSize() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int value = myRandom.nextInt();

        b.equals(false); // Make sure equals is removed.
        b.size(value);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger(MiscellaneousOperator.SIZE.getToken(), value);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#where(String)}.
     */
    @Test
    public void testWhere() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        b.equals(false); // Make sure equals is not removed.
        b.where("f == g");

        final Document e = b.asDocument();

        final DocumentBuilder db = BuilderFactory.start();
        db.addBoolean("foo", false);
        db.addJavaScript(MiscellaneousOperator.WHERE.getToken(), "f == g");

        assertEquals(db.build(), e);
    }

    /**
     * 
     * Test method for
     * {@link ConditionBuilder#within(boolean, Point2D, Point2D, Point2D, Point2D[])}
     * .
     */
    @Test
    public void testWithinBooleanPoint2DPoint2DPoint2DPoint2DArray() {
        final ConditionBuilder b = QueryBuilder.where("f");

        final boolean unique = myRandom.nextBoolean();
        final int count = myRandom.nextInt(512);
        final Point2D[] points = new Point2D[count];
        final Point2D p1 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p2 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p3 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        for (int i = 0; i < count; ++i) {
            points[i] = new Point2D.Double(myRandom.nextDouble(),
                    myRandom.nextDouble());
        }

        b.equals(false); // Make sure equals is removed.
        b.within(unique, p1, p2, p3, points);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.POLYGON);
        region.pushArray().addDouble(p1.getX()).addDouble(p1.getY());
        region.pushArray().addDouble(p2.getX()).addDouble(p2.getY());
        region.pushArray().addDouble(p3.getX()).addDouble(p3.getY());
        for (int i = 0; i < count; ++i) {
            region.pushArray().addDouble(points[i].getX())
                    .addDouble(points[i].getY());
        }
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("f", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(DocumentAssignable)}.
     */
    @Test
    public void testWithinDocumentAssignable() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 + myRandom.nextInt(1024);
        final double y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.within(GeoJson.polygon(Arrays.asList(GeoJson.p(x1, y1),
                GeoJson.p(x1, y2), GeoJson.p(x2, y2), GeoJson.p(x2, y1),
                GeoJson.p(x1, y1))));

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        wb.add(GeospatialOperator.GEOMETRY, GeoJson.polygon(Arrays.asList(
                GeoJson.p(x1, y1), GeoJson.p(x1, y2), GeoJson.p(x2, y2),
                GeoJson.p(x2, y1), GeoJson.p(x1, y1))));

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(DocumentAssignable, boolean)}.
     */
    @Test
    public void testWithinDocumentAssignableBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 + myRandom.nextInt(1024);
        final double y2 = y1 + myRandom.nextInt(1024);
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(GeoJson.polygon(Arrays.asList(GeoJson.p(x1, y1),
                GeoJson.p(x1, y2), GeoJson.p(x2, y2), GeoJson.p(x2, y1),
                GeoJson.p(x1, y1))), unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        wb.add(GeospatialOperator.GEOMETRY, GeoJson.polygon(Arrays.asList(
                GeoJson.p(x1, y1), GeoJson.p(x1, y2), GeoJson.p(x2, y2),
                GeoJson.p(x2, y1), GeoJson.p(x1, y1))));
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(double, double, double)}.
     */
    @Test
    public void testWithinDoubleDoubleDouble() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x = myRandom.nextDouble();
        final double y = myRandom.nextDouble();
        final double radius = Math.abs(myRandom.nextDouble());

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addDouble(x).addDouble(y);
        region.addDouble(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, boolean)}.
     */
    @Test
    public void testWithinDoubleDoubleDoubleBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x = myRandom.nextDouble();
        final double y = myRandom.nextDouble();
        final double radius = Math.abs(myRandom.nextDouble());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addDouble(x).addDouble(y);
        region.addDouble(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double)}.
     */
    @Test
    public void testWithinDoubleDoubleDoubleDouble() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 + myRandom.nextInt(1024);
        final double y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addDouble(x1).addDouble(y1);
        region.pushArray().addDouble(x2).addDouble(y2);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double, boolean)}.
     */
    @Test
    public void testWithinDoubleDoubleDoubleDoubleBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x1 = myRandom.nextInt(1024);
        final double y1 = myRandom.nextInt(1024);
        final double x2 = x1 - myRandom.nextInt(1024);
        final double y2 = y1 - myRandom.nextInt(1024);
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addDouble(x2).addDouble(y2);
        region.pushArray().addDouble(x1).addDouble(y1);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int)}.
     */
    @Test
    public void testWithinIntIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x = myRandom.nextInt();
        final int y = myRandom.nextInt();
        final int radius = Math.abs(myRandom.nextInt());

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addInteger(x).addInteger(y);
        region.addInteger(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, boolean)}.
     */
    @Test
    public void testWithinIntIntIntBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x = myRandom.nextInt();
        final int y = myRandom.nextInt();
        final int radius = Math.abs(myRandom.nextInt());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addInteger(x).addInteger(y);
        region.addInteger(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, int)}.
     */
    @Test
    public void testWithinIntIntIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x1 = myRandom.nextInt(1024);
        final int y1 = myRandom.nextInt(1024);
        final int x2 = x1 + myRandom.nextInt(1024);
        final int y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addInteger(x1).addInteger(y1);
        region.pushArray().addInteger(x2).addInteger(y2);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(int, int, int, int, boolean)}.
     */
    @Test
    public void testWithinIntIntIntIntBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x1 = myRandom.nextInt(1024);
        final int y1 = myRandom.nextInt(1024);
        final int x2 = x1 - myRandom.nextInt(1024);
        final int y2 = y1 - myRandom.nextInt(1024);
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addInteger(x2).addInteger(y2);
        region.pushArray().addInteger(x1).addInteger(y1);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long)}.
     */
    @Test
    public void testWithinLongLongLong() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x = myRandom.nextLong();
        final long y = myRandom.nextLong();
        final long radius = Math.abs(myRandom.nextLong());

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addLong(x).addLong(y);
        region.addLong(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, boolean)}.
     */
    @Test
    public void testWithinLongLongLongBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x = myRandom.nextLong();
        final long y = myRandom.nextLong();
        final long radius = Math.abs(myRandom.nextLong());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.CIRCLE);
        region.pushArray().addLong(x).addLong(y);
        region.addLong(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long, long)}.
     */
    @Test
    public void testWithinLongLongLongLong() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x1 = myRandom.nextInt(1024);
        final long y1 = myRandom.nextInt(1024);
        final long x2 = x1 + myRandom.nextInt(1024);
        final long y2 = y1 + myRandom.nextInt(1024);

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addLong(x1).addLong(y1);
        region.pushArray().addLong(x2).addLong(y2);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, long, boolean)}.
     */
    @Test
    public void testWithinLongLongLongLongBoolean() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x1 = myRandom.nextInt(1024);
        final long y1 = myRandom.nextInt(1024);
        final long x2 = x1 - myRandom.nextInt(1024);
        final long y2 = y1 - myRandom.nextInt(1024);
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.within(x1, y1, x2, y2, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.BOX);
        region.pushArray().addLong(x2).addLong(y2);
        region.pushArray().addLong(x1).addLong(y1);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double)}.
     */
    @Test
    public void testWithinOnSphereDoubleDoubleDouble() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x = myRandom.nextDouble();
        final double y = myRandom.nextDouble();
        final double radius = Math.abs(myRandom.nextDouble());

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addDouble(x).addDouble(y);
        region.addDouble(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double, boolean)}.
     */
    @Test
    public void testWithinOnSphereDoubleDoubleDoubleBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final double x = myRandom.nextDouble();
        final double y = myRandom.nextDouble();
        final double radius = Math.abs(myRandom.nextDouble());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addDouble(x).addDouble(y);
        region.addDouble(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(int, int, int)}.
     */
    @Test
    public void testWithinOnSphereIntIntInt() {

        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x = myRandom.nextInt();
        final int y = myRandom.nextInt();
        final int radius = Math.abs(myRandom.nextInt());

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addInteger(x).addInteger(y);
        region.addInteger(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(int, int, int, boolean)}.
     */
    @Test
    public void testWithinOnSphereIntIntIntBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final int x = myRandom.nextInt();
        final int y = myRandom.nextInt();
        final int radius = Math.abs(myRandom.nextInt());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addInteger(x).addInteger(y);
        region.addInteger(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(long, long, long)}
     * .
     */
    @Test
    public void testWithinOnSphereLongLongLong() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x = myRandom.nextLong();
        final long y = myRandom.nextLong();
        final long radius = Math.abs(myRandom.nextLong());

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addLong(x).addLong(y);
        region.addLong(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(long, long, long, boolean)}.
     */
    @Test
    public void testWithinOnSphereLongLongLongBoolean() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final long x = myRandom.nextLong();
        final long y = myRandom.nextLong();
        final long radius = Math.abs(myRandom.nextLong());
        final boolean unique = myRandom.nextBoolean();

        b.equals(false); // Make sure equals is removed.
        b.withinOnSphere(x, y, radius, unique);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb
                .pushArray(GeospatialOperator.SPHERICAL_CIRCLE);
        region.pushArray().addLong(x).addLong(y);
        region.addLong(radius);
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, unique);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(Point2D, Point2D, Point2D, Point2D[])} .
     */
    @Test
    public void testWithinPoint2DPoint2DPoint2DPoint2DArray() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final int count = myRandom.nextInt(512);
        final Point2D[] points = new Point2D[count];
        final Point2D p1 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p2 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p3 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        for (int i = 0; i < count; ++i) {
            points[i] = new Point2D.Double(myRandom.nextDouble(),
                    myRandom.nextDouble());
        }

        b.equals(false); // Make sure equals is removed.
        b.within(p1, p2, p3, points);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.POLYGON);
        region.pushArray().addDouble(p1.getX()).addDouble(p1.getY());
        region.pushArray().addDouble(p2.getX()).addDouble(p2.getY());
        region.pushArray().addDouble(p3.getX()).addDouble(p3.getY());
        for (int i = 0; i < count; ++i) {
            region.pushArray().addDouble(points[i].getX())
                    .addDouble(points[i].getY());
        }
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(Point2D, Point2D, Point2D, Point2D[])} .
     */
    @Test
    public void testWithinPoint2DPoint2DPoint2DPoint2DArrayNoExtra() {
        final ConditionBuilder b = QueryBuilder.where("foo");

        final Point2D p1 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p2 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());
        final Point2D p3 = new Point2D.Double(myRandom.nextDouble(),
                myRandom.nextDouble());

        b.equals(false); // Make sure equals is removed.
        b.within(p1, p2, p3);

        final Element e = b.buildFieldCondition();

        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentBuilder db = BuilderFactory.start();
        final DocumentBuilder wb = db
                .push(GeospatialOperator.WITHIN.getToken());
        final ArrayBuilder region = wb.pushArray(GeospatialOperator.POLYGON);
        region.pushArray().addDouble(p1.getX()).addDouble(p1.getY());
        region.pushArray().addDouble(p2.getX()).addDouble(p2.getY());
        region.pushArray().addDouble(p3.getX()).addDouble(p3.getY());
        wb.addBoolean(GeospatialOperator.UNIQUE_DOCS_MODIFIER, true);

        assertEquals(new DocumentElement("foo", db.build()), e);
    }
}
