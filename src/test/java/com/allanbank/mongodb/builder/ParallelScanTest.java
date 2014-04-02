/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;

/**
 * ParallelScanTest provides tests for the {@link ParallelScan} command.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ParallelScanTest {

    /**
     * Test method for {@link ParallelScan#ParallelScan}.
     */
    @Test
    public void testParallelScan() {
        final ParallelScan.Builder builder = new ParallelScan.Builder();
        builder.setRequestedIteratorCount(0);
        builder.setBatchSize(101010);
        builder.setReadPreference(ReadPreference.CLOSEST);

        ParallelScan request = builder.build();
        assertEquals(1, request.getRequestedIteratorCount());
        assertEquals(101010, request.getBatchSize());
        assertSame(ReadPreference.CLOSEST, request.getReadPreference());

        builder.setRequestedIteratorCount(10001);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        request = builder.build();
        assertEquals(10000, request.getRequestedIteratorCount());
        assertEquals(101010, request.getBatchSize());
        assertSame(ReadPreference.PREFER_SECONDARY, request.getReadPreference());
    }

    /**
     * Test method for {@link ParallelScan#ParallelScan}.
     */
    @Test
    public void testParallelScanMinimal() {
        final ParallelScan.Builder builder = new ParallelScan.Builder();

        final ParallelScan request = builder.build();

        assertEquals(1, request.getRequestedIteratorCount());
        assertEquals(0, request.getBatchSize());
        assertNull(request.getReadPreference());
    }
}
