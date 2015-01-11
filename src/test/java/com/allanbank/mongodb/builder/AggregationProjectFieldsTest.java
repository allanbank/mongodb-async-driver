/*
 * #%L
 * AggregationProjectFieldsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
