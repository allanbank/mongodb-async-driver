/*
 * #%L
 * JmxOperationMetricsListenerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.client.metrics.basic.BasicOperationMetrics;

/**
 * JmxOperationMetricsListenerTest provides tests for the
 * {@link JmxOperationMetricsListener} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxOperationMetricsListenerTest {

    /**
     * Test method for {@link JmxOperationMetricsListener#createMetrics(String)}
     * and
     * {@link JmxOperationMetricsListener#addedOperation(BasicOperationMetrics)}
     * .
     */
    @Test
    public void testAddedOperation() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);

        mockSupport.register(anyObject(AbstractMetrics.class), eq("operation"),
                eq("name"));

        replay(mockSupport);

        final JmxOperationMetricsListener listener = new JmxOperationMetricsListener(
                mockSupport);
        final BasicOperationMetrics metrics = listener.createMetrics("name");

        listener.addedOperation(metrics);

        verify(mockSupport);
    }

    /**
     * Test method for
     * {@link JmxOperationMetricsListener#addedOperation(BasicOperationMetrics)}
     * .
     */
    @Test
    public void testAddedServerNotJmx() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);

        replay(mockSupport);

        final JmxOperationMetricsListener listener = new JmxOperationMetricsListener(
                mockSupport);
        final BasicOperationMetrics metrics = new BasicOperationMetrics("name");

        listener.addedOperation(metrics);

        verify(mockSupport);
    }

}
