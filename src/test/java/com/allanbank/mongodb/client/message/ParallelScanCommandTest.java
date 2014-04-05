/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.ParallelScan;
import com.allanbank.mongodb.client.Message;

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

    /**
     * Test method for {@link AggregateCommand#equals(Object)} and
     * {@link AggregateCommand#hashCode()} .
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {
        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();

        for (final Document document : Arrays.asList(doc1, doc2, doc3)) {

            // The command document and command are linked so we don't compare
            // the non-document.
            final ParallelScan command = ParallelScan.builder()
                    .requestedIteratorCount(1).build();

            for (final String databaseName : Arrays.asList("n1", "n2", "n3")) {
                for (final String collectionName : Arrays.asList("c1", "c2",
                        "c3")) {
                    for (final ReadPreference readPreference : Arrays
                            .asList(ReadPreference.PRIMARY)) {

                        objs1.add(new ParallelScanCommand(command,
                                databaseName, collectionName, document,
                                readPreference));
                        objs2.add(new ParallelScanCommand(command,
                                databaseName, collectionName, document,
                                readPreference));
                    }
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Message obj1 = objs1.get(i);
            Message obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new Command("db", doc1)));
        }
    }
}
