/*
 * #%L
 * ListCollectionsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;

/**
 * ListCollectionsTest provides tests for the {@link ListCollections} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */

public class ListCollectionsTest {

    /**
     * Test method for {@link ListCollections#getBatchSize()}.
     */
    @Test
    public void testGetBatchSize() {
        final int batchSize = new Random(System.currentTimeMillis())
        .nextInt(Integer.MAX_VALUE);

        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(builder.build().getBatchSize(), is(0));

        builder.batchSize(batchSize);
        assertThat(builder.build().getBatchSize(), is(batchSize));

        builder.reset();
        assertThat(builder.build().getBatchSize(), is(0));
    }

    /**
     * Test method for {@link ListCollections#getLimit()}.
     */
    @Test
    public void testGetLimit() {
        final int limit = new Random(System.currentTimeMillis())
        .nextInt(Integer.MAX_VALUE);

        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(builder.build().getLimit(), is(0));

        builder.limit(limit);
        assertThat(builder.build().getLimit(), is(limit));

        builder.reset();
        assertThat(builder.build().getLimit(), is(0));
    }

    /**
     * Test method for {@link ListCollections#getMaximumTimeMilliseconds()}.
     */
    @Test
    public void testGetMaximumTimeMilliseconds() {
        final long time = new Random(System.currentTimeMillis())
        .nextInt(Integer.MAX_VALUE);

        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(builder.build().getMaximumTimeMilliseconds(), is(0L));

        builder.maximumTime(time, TimeUnit.MILLISECONDS);
        assertThat(builder.build().getMaximumTimeMilliseconds(), is(time));

        builder.reset();
        assertThat(builder.build().getMaximumTimeMilliseconds(), is(0L));
    }

    /**
     * Test method for {@link ListCollections#getQuery()}.
     */
    @Test
    public void testGetQuery() {
        final Document query = QueryBuilder.where("name").equals("abc")
                .asDocument();

        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(builder.build().getQuery(), sameInstance(Find.ALL));

        builder.query(query);
        assertThat(builder.build().getQuery(), sameInstance(query));

        builder.reset();
        assertThat(builder.build().getQuery(), sameInstance(Find.ALL));
    }

    /**
     * Test method for {@link ListCollections#getReadPreference()}.
     */
    @Test
    public void testGetReadPreference() {
        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(builder.build().getReadPreference(), nullValue());

        builder.readPreference(ReadPreference.PREFER_SECONDARY);
        assertThat(builder.build().getReadPreference(),
                sameInstance(ReadPreference.PREFER_SECONDARY));

        builder.reset();
        assertThat(builder.build().getReadPreference(), nullValue());
    }

    /**
     * Test method for {@link ListCollections#toString()}.
     */
    @Test
    public void testToString() {
        final ListCollections.Builder builder = ListCollections.builder();

        assertThat(
                builder.toString(),
                is("listCollections[query={}, batchSize=0, limit=0, maxTime=0 ms, readPreference=null]"));
    }
}