/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * TextTest provides tests for the {@link Text} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("boxing")
public class TextTest {

    /**
     * Test method for {@link Text#builder()}.
     */
    @Test
    public void testFull() {

        final Text command = Text.builder().searchTerm("bar").language("l")
                .limit(10).query(where("f").equals(false))
                .readPreference(ReadPreference.SECONDARY)
                .returnFields(BuilderFactory.start().add("f", 1).add("_id", 0))
                .build();

        assertThat(command.getSearchTerm(), is("bar"));
        assertThat(command.getLanguage(), is("l"));
        assertThat(command.getLimit(), is(10));
        assertThat(command.getQuery(),
                is(where("f").equals(false).asDocument()));
        assertThat(command.getReadPreference(), is(ReadPreference.SECONDARY));
        assertThat(command.getReturnFields(),
                is(BuilderFactory.start().add("f", 1).add("_id", 0).build()));
    }

    /**
     * Test method for {@link Text.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsDefault() {
        final Text.Builder b = Text.builder().searchTerm("foo");
        final Text command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(0L));
    }

    /**
     * Test method for {@link Text.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaFluent() {
        final Random random = new Random(System.currentTimeMillis());
        final Text.Builder b = Text.builder().searchTerm("foo");

        final long value = random.nextLong();
        b.maximumTime(value, TimeUnit.MILLISECONDS);

        final Text command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for {@link Text.Builder#setMaximumTimeMilliseconds(long)} .
     */
    @Test
    public void testMaximumTimeMillisecondsViaSetter() {
        final Random random = new Random(System.currentTimeMillis());
        final Text.Builder b = Text.builder().searchTerm("foo");

        final long value = random.nextLong();
        b.setMaximumTimeMilliseconds(value);

        final Text command = b.build();

        assertThat(command.getMaximumTimeMilliseconds(), is(value));
    }

    /**
     * Test method for {@link Text#builder()}.
     */
    @Test
    public void testMinimal() {

        final Text command = Text.builder().searchTerm("foo").build();

        assertThat(command.getSearchTerm(), is("foo"));

        assertThat(command.getLanguage(), nullValue());
        assertThat(command.getLimit(), is(0));
        assertThat(command.getQuery(), nullValue());
        assertThat(command.getReadPreference(), nullValue());
        assertThat(command.getReturnFields(), nullValue());
    }

    /**
     * Test method for {@link Text.Builder#reset()}.
     */
    @Test
    public void testReset() {

        final Text.Builder b = Text.builder().searchTerm("bar").language("l")
                .limit(10).query(where("f").equals(false))
                .readPreference(ReadPreference.SECONDARY)
                .returnFields(BuilderFactory.start().add("f", 1).add("_id", 0));
        Text command = b.build();

        assertThat(command.getSearchTerm(), is("bar"));
        assertThat(command.getLanguage(), is("l"));
        assertThat(command.getLimit(), is(10));
        assertThat(command.getQuery(),
                is(where("f").equals(false).asDocument()));
        assertThat(command.getReadPreference(), is(ReadPreference.SECONDARY));
        assertThat(command.getReturnFields(),
                is(BuilderFactory.start().add("f", 1).add("_id", 0).build()));

        command = b.reset().searchTerm("baz").build();

        assertThat(command.getSearchTerm(), is("baz"));

        assertThat(command.getLanguage(), nullValue());
        assertThat(command.getLimit(), is(0));
        assertThat(command.getQuery(), nullValue());
        assertThat(command.getReadPreference(), nullValue());
        assertThat(command.getReturnFields(), nullValue());
    }

    /**
     * Test method for {@link Text.Builder}.
     */
    @Test
    public void testSetNulls() {

        final Text.Builder b = Text.builder().searchTerm("bar").language("l")
                .limit(10).query(where("f").equals(false))
                .readPreference(ReadPreference.SECONDARY)
                .returnFields(BuilderFactory.start().add("f", 1).add("_id", 0));
        Text command = b.build();

        assertThat(command.getSearchTerm(), is("bar"));
        assertThat(command.getLanguage(), is("l"));
        assertThat(command.getLimit(), is(10));
        assertThat(command.getQuery(),
                is(where("f").equals(false).asDocument()));
        assertThat(command.getReadPreference(), is(ReadPreference.SECONDARY));
        assertThat(command.getReturnFields(),
                is(BuilderFactory.start().add("f", 1).add("_id", 0).build()));

        command = b.query(null).readPreference(null).returnFields(null)
                .language(null).build();

        assertThat(command.getSearchTerm(), is("bar"));
        assertThat(command.getLimit(), is(10));

        assertThat(command.getLanguage(), nullValue());
        assertThat(command.getQuery(), nullValue());
        assertThat(command.getReadPreference(), nullValue());
        assertThat(command.getReturnFields(), nullValue());
    }

    /**
     * Test method for {@link Text#Text}.
     */
    @Test
    public void testTextWithEmptySearchTerm() {

        final Text.Builder builder = Text.builder().searchTerm("");

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a Text command without a search term.",
                built);
    }

    /**
     * Test method for {@link Text#Text}.
     */
    @Test
    public void testTextWithNullSearchTerm() {

        final Text.Builder builder = Text.builder();

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a Text command without a search term.",
                built);
    }
}
