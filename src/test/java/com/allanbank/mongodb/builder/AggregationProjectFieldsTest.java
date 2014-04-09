/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.AggregationProjectFields.include;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.IntegerElement;

/**
 * AggregationProjectFieldsTest provides tests for the
 * {@link AggregationProjectFields} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationProjectFieldsTest {

    /**
     * Test method for {@link AggregationProjectFields#include(String[])}.
     */
    @Test
    public void testInclude() {
        assertEquals(Arrays.asList(new IntegerElement("f", 1),
                new IntegerElement("g", 1)), include("f", "g").toElements());
    }

    /**
     * Test method for
     * {@link AggregationProjectFields#includeWithoutId(String[])}.
     */
    @Test
    public void testIncludeWithoutId() {

        assertEquals(Arrays.asList(new IntegerElement("_id", 0),
                new IntegerElement("f", 1)), includeWithoutId("f").toElements());
    }

}
