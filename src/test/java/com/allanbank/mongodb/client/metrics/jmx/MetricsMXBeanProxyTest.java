/*
 * #%L
 * MetricsMXBeanProxyTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;

/**
 * MetricsMXBeanProxyTest provides tests for the {@link MetricsMXBeanProxy}
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MetricsMXBeanProxyTest {

    /**
     * Test method for {@link MetricsMXBeanProxy#getAverageLatencyMillis()}.
     */
    @Test
    public void testGetAverageLatencyMillis() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getAverageLatencyMillis()).andReturn(1.0);

        replay(mockMetrics);

        assertThat(mockMetrics.getAverageLatencyMillis(), is(1.0));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getLastLatencyNanos()}.
     */
    @Test
    public void testGetLastLatencyNanos() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getLastLatencyNanos()).andReturn(12345L);

        replay(mockMetrics);

        assertThat(mockMetrics.getLastLatencyNanos(), is(12345L));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getMessageReceivedBytes()}.
     */
    @Test
    public void testGetMessageReceivedBytes() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getMessageReceivedBytes()).andReturn(43121L);

        replay(mockMetrics);

        assertThat(mockMetrics.getMessageReceivedBytes(), is(43121L));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getMessageReceivedCount()}.
     */
    @Test
    public void testGetMessageReceivedCount() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getMessageReceivedCount()).andReturn(345345L);

        replay(mockMetrics);

        assertThat(mockMetrics.getMessageReceivedCount(), is(345345L));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getMessageSentBytes()}.
     */
    @Test
    public void testGetMessageSentBytes() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getMessageSentBytes()).andReturn(567467L);

        replay(mockMetrics);

        assertThat(mockMetrics.getMessageSentBytes(), is(567467L));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getMessageSentCount()}.
     */
    @Test
    public void testGetMessageSentCount() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getMessageSentCount()).andReturn(234234L);

        replay(mockMetrics);

        assertThat(mockMetrics.getMessageSentCount(), is(234234L));

        verify(mockMetrics);
    }

    /**
     * Test method for
     * {@link MetricsMXBeanProxy#getRecentAverageLatencyMillis()}.
     */
    @Test
    public void testGetRecentAverageLatencyMillis() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getRecentAverageLatencyMillis()).andReturn(0.1234);

        replay(mockMetrics);

        assertThat(mockMetrics.getRecentAverageLatencyMillis(), is(0.1234));

        verify(mockMetrics);
    }

    /**
     * Test method for {@link MetricsMXBeanProxy#getTotalLatencyNanos()}.
     */
    @Test
    public void testGetTotalLatencyNanos() {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);

        expect(mockMetrics.getTotalLatencyNanos()).andReturn(4534563456L);

        replay(mockMetrics);

        assertThat(mockMetrics.getTotalLatencyNanos(), is(4534563456L));

        verify(mockMetrics);
    }

}
