/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static com.allanbank.mongodb.builder.expression.Expressions.add;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Aggregate.Builder;

/**
 * AggregateBuilderTest provides tests of the {@link Aggregate.Builder} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregateBuilderTest extends Builder {

    /**
     * Test method for
     * {@link Aggregate.Builder#group(AggregationGroupId, AggregationGroupField[])}
     * .
     */
    @Test
    public void testGroupAggregationGroupIdAggregationGroupFieldArray() {
        final Aggregate.Builder builder = Aggregate.builder();
        builder.group(id("a"), set("d").average("e"));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        final DocumentBuilder db = expected.push().push("$group");
        db.addString("_id", "$a");
        db.push("d").addString("$avg", "$e");

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#group(AggregationGroupId.Builder, AggregationGroupField[])}
     * .
     */
    @Test
    public void testGroupBuilderAggregationGroupFieldArray() {

        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.group(id().addField("a").addField("b", "c"),
                set("d").average("e"));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        final DocumentBuilder db = expected.push().push("$group");
        db.push("_id").addString("a", "$a").addString("b", "$c");
        db.push("d").addString("$avg", "$e");

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());
    }

    /**
     * Test method for {@link Aggregate.Builder#group(DocumentAssignable)} .
     */
    @Test
    public void testGroupDocumentAssignable() {
        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger("f", 1);

        final DocumentAssignable da = db;

        final Aggregate.Builder b = new Aggregate.Builder();

        b.group(da);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("$group", first.getName());

        assertEquals(new DocumentElement("$group", db.build()), first);

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#group(DocumentAssignable, AggregationGroupField[])}
     * .
     */
    @Test
    public void testGroupDocumentAssignableAggregationGroupFieldArray() {
        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.group(id().addField("a").addField("b", "c").build(), set("d")
                .average("e"));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        final DocumentBuilder db = expected.push().push("$group");
        db.push("_id").addString("a", "$a").addString("b", "$c");
        db.push("d").addString("$avg", "$e");

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());
    }

    /**
     * Test method for {@link Aggregate.Builder#limit(int)}.
     */
    @Test
    public void testLimitInt() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.limit(100);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(IntegerElement.class));
        assertEquals("$limit", first.getName());
        assertEquals(100, ((IntegerElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#limit(long)}.
     */
    @Test
    public void testLimitLong() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.limit(100L);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(LongElement.class));
        assertEquals("$limit", first.getName());
        assertEquals(100L, ((LongElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#match(DocumentAssignable)} .
     */
    @Test
    public void testMatch() {
        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger("f", 1);

        final DocumentAssignable da = db;

        final Aggregate.Builder b = new Aggregate.Builder();

        b.match(da);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("$match", first.getName());

        assertEquals(new DocumentElement("$match", db.build()), first);

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsDefault() {
        final Aggregate.Builder b = new Aggregate.Builder();
        final Aggregate command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(0L));
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaFluent() {
        final Random random = new Random(System.currentTimeMillis());
        final Aggregate.Builder b = new Aggregate.Builder();

        final long value = random.nextLong();
        b.maximumTime(value, TimeUnit.MILLISECONDS);

        final Aggregate command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaSetter() {
        final Random random = new Random(System.currentTimeMillis());
        final Aggregate.Builder b = new Aggregate.Builder();

        final long value = random.nextLong();
        b.setMaximumTimeMilliseconds(value);

        final Aggregate command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#project(AggregationProjectFields, Element[])} .
     */
    @Test
    public void testProjectAggregationProjectFieldsElementArray() {
        final int interval = 100000;

        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.project(includeWithoutId("a"), set("f", field("f")),
                set("g", field("h")), set("i", field("j")),
                set("k", add(constant(0), constant(interval))));

        // Now the old fashioned way.
        final ArrayBuilder expected = BuilderFactory.startArray();
        final DocumentBuilder db = expected.push().push("$project");
        db.addInteger("_id", 0);
        db.addInteger("a", 1);
        db.addString("f", "$f");
        db.addString("g", "$h");
        db.addString("i", "$j");
        db.push("k").pushArray("$add").addInteger(0).addInteger(interval);

        assertEquals(Arrays.asList(expected.build()), builder.build()
                .getPipeline());
    }

    /**
     * Test method for {@link Aggregate.Builder#project(DocumentAssignable)} .
     */
    @Test
    public void testProjectDocumentAssignable() {
        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger("f", 1);

        final DocumentAssignable da = db;

        final Aggregate.Builder b = new Aggregate.Builder();

        b.project(da);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("$project", first.getName());

        assertEquals(new DocumentElement("$project", db.build()), first);

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#reset()}.
     */
    @Test
    public void testReset() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.skip(100);

        Aggregate a = b.build();

        List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        b.reset();

        a = b.build();
        pipeline = a.getPipeline();
        assertEquals(0, pipeline.size());
    }

    /**
     * Test method for {@link Aggregate.Builder#skip(int)}.
     */
    @Test
    public void testSkipInt() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.skip(100);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(IntegerElement.class));
        assertEquals("$skip", first.getName());
        assertEquals(100, ((IntegerElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#skip(long)}.
     */
    @Test
    public void testSkipLong() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.skip(100L);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(LongElement.class));
        assertEquals("$skip", first.getName());
        assertEquals(100L, ((LongElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#sort(IntegerElement[])} .
     */
    @Test
    public void testSortIntegerElementArray() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.sort(Sort.asc("f"), Sort.desc("g"));

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("$sort", first.getName());

        assertEquals(new IntegerElement("f", 1), ((DocumentElement) first)
                .iterator().next());
        assertEquals(new IntegerElement("g", -1),
                ((DocumentElement) first).get("g"));

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#sort(String[])}.
     */
    @Test
    public void testSortStringArray() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.sort("f", "g");

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("$sort", first.getName());

        assertEquals(new IntegerElement("f", 1), ((DocumentElement) first)
                .iterator().next());
        assertEquals(new IntegerElement("g", 1),
                ((DocumentElement) first).get("g"));

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for
     * {@link Aggregate.Builder#step(String, DocumentAssignable)} .
     */
    @Test
    public void testStepStringDocumentAssignable() {
        final DocumentBuilder db = BuilderFactory.start();
        db.addInteger("f", 1);

        final DocumentAssignable da = db;

        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", da);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("foo", first.getName());

        assertEquals(new DocumentElement("foo", db.build()), first);

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, double)}.
     */
    @Test
    public void testStepStringDouble() {

        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", 1.023);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DoubleElement.class));
        assertEquals("foo", first.getName());

        assertEquals(1.023, ((DoubleElement) first).getValue(), 0.001);

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, Element[])} .
     */
    @Test
    public void testStepStringElementArray() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", new IntegerElement("f", 1), new IntegerElement("g", -1));

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("foo", first.getName());

        assertEquals(new IntegerElement("f", 1), ((DocumentElement) first)
                .iterator().next());
        assertEquals(new IntegerElement("g", -1),
                ((DocumentElement) first).get("g"));

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, int)}.
     */
    @Test
    public void testStepStringInt() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", 1023);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(IntegerElement.class));
        assertEquals("foo", first.getName());

        assertEquals(1023, ((IntegerElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, List)}.
     */
    @Test
    public void testStepStringListOfElement() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", Arrays.asList((Element) new IntegerElement("f", 1),
                new IntegerElement("g", -1)));

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(DocumentElement.class));
        assertEquals("foo", first.getName());

        assertEquals(new IntegerElement("f", 1), ((DocumentElement) first)
                .iterator().next());
        assertEquals(new IntegerElement("g", -1),
                ((DocumentElement) first).get("g"));

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, long)}.
     */
    @Test
    public void testStepStringLong() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", 1023L);

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(LongElement.class));
        assertEquals("foo", first.getName());

        assertEquals(1023L, ((LongElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#step(String, String)}.
     */
    @Test
    public void testStepStringString() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.step("foo", "bar");

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(StringElement.class));
        assertEquals("foo", first.getName());

        assertEquals("bar", ((StringElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#unwind(String)}.
     */
    @Test
    public void testUnwindWithDollarSign() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.unwind("$bar");

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(StringElement.class));
        assertEquals("$unwind", first.getName());

        assertEquals("$bar", ((StringElement) first).getValue());

        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link Aggregate.Builder#unwind(String)}.
     */
    @Test
    public void testUnwindWithoutDollarSign() {
        final Aggregate.Builder b = new Aggregate.Builder();

        b.unwind("bar");

        final Aggregate a = b.build();

        final List<Element> pipeline = a.getPipeline();
        assertEquals(1, pipeline.size());

        final Element e = pipeline.get(0);
        assertEquals("0", e.getName());
        assertThat(e, instanceOf(DocumentElement.class));

        final DocumentElement d = (DocumentElement) e;
        final Iterator<Element> iter = d.iterator();
        assertTrue(iter.hasNext());

        final Element first = iter.next();
        assertThat(first, instanceOf(StringElement.class));
        assertEquals("$unwind", first.getName());

        assertEquals("$bar", ((StringElement) first).getValue());

        assertFalse(iter.hasNext());
    }

}
