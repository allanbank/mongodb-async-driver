/*
 * #%L
 * NoOpMongoClientMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.metrics.noop;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.client.metrics.NoOpMongoMessageListener;

/**
 * NoOpMongoClientMetricsTest provides tests for the
 * {@link NoOpMongoClientMetrics} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpMongoClientMetricsTest {

    /**
     * Test method for {@link NoOpMongoClientMetrics#close()}.
     */
    @Test
    public void testClose() {
        final NoOpMongoClientMetrics clientMetrics = new NoOpMongoClientMetrics();

        // Nothing to see here.
        clientMetrics.close();
    }

    /**
     * Test method for {@link NoOpMongoClientMetrics#connectionClosed} .
     */
    @Test
    public void testConnectionClosed() {
        final NoOpMongoClientMetrics clientMetrics = new NoOpMongoClientMetrics();

        // Nothing to see here.
        clientMetrics.connectionClosed(null);

        // For Closeable.
        clientMetrics.close();
    }

    /**
     * Test method for {@link NoOpMongoClientMetrics#newConnection(String)}.
     */
    @Test
    public void testNewConnection() {
        final NoOpMongoClientMetrics clientMetrics = new NoOpMongoClientMetrics();

        // Nothing to see here.
        assertThat(clientMetrics.newConnection(null),
                sameInstance(NoOpMongoMessageListener.NO_OP));

        // For Closeable.
        clientMetrics.close();
    }

    /**
     * Test method for {@link NoOpMongoClientMetrics#setMessageListener} .
     */
    @Test
    public void testSetMessageListener() {
        final NoOpMongoClientMetrics clientMetrics = new NoOpMongoClientMetrics();

        // Nothing to see here.
        clientMetrics.setMessageListener(null);

        // For Closeable.
        clientMetrics.close();
    }

    /**
     * Test method for {@link NoOpMongoClientMetrics#toString()}.
     */
    @Test
    public void testToString() {
        final NoOpMongoClientMetrics clientMetrics = new NoOpMongoClientMetrics();

        // Nothing to see here.
        assertThat(clientMetrics.toString(), is("Metrics not being collected."));

        // For Closeable.
        clientMetrics.close();
    }
}
