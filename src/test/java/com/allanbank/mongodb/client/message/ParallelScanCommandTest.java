/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.ParallelScan;

/**
 * ParallelScanCommandTest provides tests for the {@link ParallelScanCommand}
 * class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ParallelScanCommandTest {

    /**
     * Test method for
     * {@link ParallelScanCommand#ParallelScanCommand(ParallelScan, String, String, Document, ReadPreference)}
     * .
     */
    @Test
    public void testAggregationCommand() {
        final ParallelScan aggregation = ParallelScan.builder().batchSize(1234)
                .requestedIteratorCount(4567).build();
        final Document doc = BuilderFactory.start()
                .add("parallelCollectionScan", "collection").build();

        final ParallelScanCommand command = new ParallelScanCommand(
                aggregation, "db", "collection", doc,
                ReadPreference.PREFER_PRIMARY);

        assertThat(command.getOperationName(), is("parallelCollectionScan"));
        assertThat(command.getDatabaseName(), is("db"));
        assertThat(command.getCollectionName(), is("collection"));
        assertThat(command.getBatchSize(), is(1234));
        assertThat(command.getLimit(), is(0));
    }
}
