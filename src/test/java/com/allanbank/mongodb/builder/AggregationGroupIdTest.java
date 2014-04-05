/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.AggregationGroupId.constantId;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * AggregationGroupIdTest provides tests for the {@link AggregationGroupId} and
 * {@link AggregationGroupId.Builder} classes.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationGroupIdTest {

    /**
     * Test method for {@link AggregationGroupId#constantId(String)}.
     */
    @Test
    public void testConstantId() {
        assertEquals(new StringElement("_id", "f"), constantId("f").toElement());
    }

    /**
     * Test method for {@link AggregationGroupId#id()}.
     */
    @Test
    public void testId() {
        assertEquals(new DocumentElement("_id",
                new StringElement("bar", "$bar"), new StringElement("baz",
                        "$baz")),
                        id().addField("bar", "bar").addField("baz", "$baz").buildId()
                        .toElement());

        assertEquals(new DocumentElement("_id",
                new StringElement("bar", "$bar"), new StringElement("baz",
                        "$baz")), id().addField("bar").addField("$baz")
                        .buildId().toElement());
    }

    /**
     * Test method for {@link AggregationGroupId#id(String)}.
     */
    @Test
    public void testIdString() {
        assertEquals(new StringElement("_id", "$f"), id("f").toElement());
        assertEquals(new StringElement("_id", "$g"), id("$g").toElement());
    }

}
