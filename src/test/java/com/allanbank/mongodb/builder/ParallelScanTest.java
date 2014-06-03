/*
 * #%L
 * ParallelScanTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
