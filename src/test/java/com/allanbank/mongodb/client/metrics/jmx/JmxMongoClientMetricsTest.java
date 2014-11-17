/*
 * #%L
 * JmxMongoClientMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.metrics.jmx;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.PrintWriter;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.basic.BasicConnectionMetrics;

/**
 * JmxMongoClientMetricsTest provides tests for the
 * {@link JmxMongoClientMetrics} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxMongoClientMetricsTest {

    /**
     * Test method for
     * {@link JmxMongoClientMetrics#connectionClosed(MongoMessageListener)}.
     */
    @Test
    public void testConnectionClosed() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);

        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("MongoClient"), anyObject(String.class));
        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("connection"), eq("server"), anyInt());

        mockSupport.unregister(eq("connection"), eq("server"), anyInt());

        // Close
        mockSupport.unregister(eq("MongoClient"), anyObject(String.class));

        replay(mockSupport);

        final JmxMongoClientMetrics clientMetrics = new JmxMongoClientMetrics(
                mockSupport);

        final JmxConnectionMetrics connectionMetrics = clientMetrics
                .newConnection("server");
        clientMetrics.connectionClosed(connectionMetrics);

        // For Closeable.
        clientMetrics.close();

        verify(mockSupport);
    }

    /**
     * Test method for
     * {@link JmxMongoClientMetrics#connectionClosed(MongoMessageListener)}.
     */
    @Test
    public void testConnectionClosedNotJmx() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);
        final MongoMessageListener mockListener = createMock(MongoMessageListener.class);

        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("MongoClient"), anyObject(String.class));

        // Close
        mockSupport.unregister(eq("MongoClient"), anyObject(String.class));

        replay(mockSupport, mockListener);

        final JmxMongoClientMetrics clientMetrics = new JmxMongoClientMetrics(
                mockSupport);

        final BasicConnectionMetrics connectionMetrics = new BasicConnectionMetrics(
                mockListener);
        clientMetrics.connectionClosed(connectionMetrics);

        // For Closeable.
        clientMetrics.close();

        verify(mockSupport, mockListener);
    }

    /**
     * Test method for {@link JmxMongoClientMetrics#newConnection(String)}.
     */
    @Test
    public void testNewConnectionString() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);

        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("MongoClient"), anyObject(String.class));
        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("connection"), eq("server"), anyInt());

        // Close
        mockSupport.unregister(eq("MongoClient"), anyObject(String.class));

        replay(mockSupport);

        final JmxMongoClientMetrics clientMetrics = new JmxMongoClientMetrics(
                mockSupport);

        // Nothing to see here.
        assertThat(clientMetrics.newConnection("server"),
                Matchers.instanceOf(JmxConnectionMetrics.class));

        // For Closeable.
        clientMetrics.close();

        verify(mockSupport);
    }

    /**
     * Test method for {@link JmxMongoClientMetrics#writeTo(PrintWriter)}.
     */
    @Test
    public void testWriteToPrintWriter() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);

        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("MongoClient"), anyObject(String.class));
        mockSupport.unregister(eq("MongoClient"), anyObject(String.class));

        replay(mockSupport);
        final JmxMongoClientMetrics metrics = new JmxMongoClientMetrics(
                mockSupport);

        assertThat(
                metrics.toString(),
                is("Global[sentBytes=0, sentCount=0, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n\n\n\n"
                        + "Note: Realtime metrics also available via JMX."));

        metrics.close();
        verify(mockSupport);
    }
}
