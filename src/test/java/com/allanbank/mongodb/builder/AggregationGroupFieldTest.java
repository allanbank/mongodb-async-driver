/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * AggregationGroupFieldTest provides tests for the
 * {@link AggregationGroupField} and {@link AggregationGroupField.Builder}
 * classes
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationGroupFieldTest {

    /**
     * Test method for {@link AggregationGroupField.Builder#addToSet(String)}.
     */
    @Test
    public void testAddToSet() {
        assertEquals(new DocumentElement("f", new StringElement("$addToSet",
                "$value")), set("f").addToSet("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#all} .
     */
    @Test
    public void testAll() {
        assertEquals(new DocumentElement("f", new StringElement("$push",
                "$value")), set("f").all("$value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#as(String, int)}.
     */
    @Test
    public void testAsStringInt() {
        assertEquals(new DocumentElement("f", new IntegerElement("op", 1)),
                set("f").as("op", 1).toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#as(String, String)}.
     */
    @Test
    public void testAsStringString() {
        assertEquals(
                new DocumentElement("f", new StringElement("op", "$value")),
                set("f").as("op", "$value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#average}.
     */
    @Test
    public void testAverage() {
        assertEquals(new DocumentElement("f", new StringElement("$avg",
                "$value")), set("f").average("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#count}.
     */
    @Test
    public void testCount() {
        assertEquals(new DocumentElement("f", new IntegerElement("$sum", 1)),
                set("f").count().toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#first}.
     */
    @Test
    public void testFirst() {
        assertEquals(new DocumentElement("f", new StringElement("$first",
                "$value")), set("f").first("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#last}.
     */
    @Test
    public void testLast() {
        assertEquals(new DocumentElement("f", new StringElement("$last",
                "$value")), set("f").last("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#maximum}.
     */
    @Test
    public void testMaximum() {
        assertEquals(new DocumentElement("f", new StringElement("$max",
                "$value")), set("f").maximum("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#minimum}.
     */
    @Test
    public void testMinimum() {
        assertEquals(new DocumentElement("f", new StringElement("$min",
                "$value")), set("f").minimum("value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#push} .
     */
    @Test
    public void testPush() {
        assertEquals(new DocumentElement("f", new StringElement("$push",
                "$value")), set("f").push("$value").toElement());
    }

    /**
     * Test method for {@link AggregationGroupField.Builder#sum}.
     */
    @Test
    public void testSum() {
        assertEquals(new DocumentElement("f", new StringElement("$sum",
                "$value")), set("f").sum("value").toElement());
    }

    /**
     * Test method for
     * {@link AggregationGroupField.Builder#uniqueValuesOf(String)}.
     */
    @Test
    public void testUniqueValuesOf() {
        assertEquals(new DocumentElement("f", new StringElement("$addToSet",
                "$value")), set("f").uniqueValuesOf("value").toElement());
    }
}
