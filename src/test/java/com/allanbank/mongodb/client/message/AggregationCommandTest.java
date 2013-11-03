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
import com.allanbank.mongodb.builder.Aggregation;

/**
 * AggregationCommandTest provides tests for the {@link AggregationCommand}
 * class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AggregationCommandTest {

    /**
     * Test method for
     * {@link AggregationCommand#AggregationCommand(Aggregation, String, String, Document, ReadPreference, Version)}
     * .
     */
    @Test
    public void testAggregationCommand() {
        final Aggregation aggregation = Aggregation.builder().batchSize(1234)
                .cursorLimit(4567).build();
        final Document doc = BuilderFactory.start()
                .add("aggregate", "collection").build();

        final AggregationCommand command = new AggregationCommand(aggregation,
                "db", "collection", doc, ReadPreference.PREFER_PRIMARY,
                Version.VERSION_2_4);

        assertThat(command.getOperationName(), is("aggregate"));
        assertThat(command.getDatabaseName(), is("db"));
        assertThat(command.getCollectionName(), is("collection"));
        assertThat(command.getBatchSize(), is(1234));
        assertThat(command.getLimit(), is(4567));
    }
}
