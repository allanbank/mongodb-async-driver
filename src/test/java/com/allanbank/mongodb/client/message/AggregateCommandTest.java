/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.client.VersionRange;

/**
 * AggregateCommandTest provides tests for the {@link AggregateCommand} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregateCommandTest {

    /**
     * Test method for
     * {@link AggregateCommand#AggregateCommand(Aggregate, String, String, Document, ReadPreference, VersionRange)}
     * .
     */
    @Test
    public void testAggregationCommand() {
        final Aggregate aggregation = Aggregate.builder().batchSize(1234)
                .cursorLimit(4567).build();
        final Document doc = BuilderFactory.start()
                .add("aggregate", "collection").build();

        final AggregateCommand command = new AggregateCommand(aggregation,
                "db", "collection", doc, ReadPreference.PREFER_PRIMARY,
                VersionRange.minimum(Version.VERSION_2_4));

        assertThat(command.getOperationName(), is("aggregate"));
        assertThat(command.getDatabaseName(), is("db"));
        assertThat(command.getCollectionName(), is("collection"));
        assertThat(command.getBatchSize(), is(1234));
        assertThat(command.getLimit(), is(4567));
    }
}
