/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * CountTest provides tests for the {@link Count} command.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CountTest {

    /**
     * Test method for {@link Count#Count}.
     */
    @Test
    public void testCount() {
        final Document query = Count.ALL;

        final Count.Builder builder = new Count.Builder();
        builder.setQuery(query);
        builder.setReadPreference(ReadPreference.CLOSEST);

        Count request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());

        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());
    }

    /**
     * Test method for {@link Count#Count}.
     */
    @Test
    public void testCountMinimal() {
        final Count.Builder builder = new Count.Builder();

        final Count request = builder.build();

        assertSame(Count.ALL, request.getQuery());
        assertNull(request.getReadPreference());
    }

    /**
     * Test method for {@link Count#Count}.
     */
    @Test
    public void testCountReset() {
        final Document query = BuilderFactory.start().add("a", 1).build();

        final Count.Builder builder = new Count.Builder();
        builder.setQuery(query);
        builder.setReadPreference(ReadPreference.CLOSEST);
        builder.setMaximumTimeMilliseconds(1000);

        Count request = builder.build();
        assertSame(query, request.getQuery());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());
        assertEquals(1000, request.getMaximumTimeMilliseconds());

        builder.reset();

        request = builder.build();
        assertSame(Count.ALL, request.getQuery());
        assertNull(request.getReadPreference());
        assertEquals(0, request.getMaximumTimeMilliseconds());
    }

    /**
     * Test method for {@link Count.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsDefault() {
        final Count.Builder b = Count.builder();
        final Count command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(0L));
    }

    /**
     * Test method for {@link Count.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaFluent() {
        final Random random = new Random(System.currentTimeMillis());
        final Count.Builder b = Count.builder();

        final long value = Math.abs(random.nextLong()) + 1;
        b.maximumTime(value, TimeUnit.MILLISECONDS);

        final Count command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for {@link Count.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaSetter() {
        final Random random = new Random(System.currentTimeMillis());
        final Count.Builder b = Count.builder();

        final long value = random.nextLong();
        b.setMaximumTimeMilliseconds(value);

        final Count command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }
}
