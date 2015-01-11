/*
 * #%L
 * AggregationGroupIdTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
