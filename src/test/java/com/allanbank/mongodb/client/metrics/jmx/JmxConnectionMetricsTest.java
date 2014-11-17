/*
 * #%L
 * JmxConnectionMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * JmxConnectionMetricsTest provides tests for the {@link JmxConnectionMetrics}
 * class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxConnectionMetricsTest {

    /**
     * Test method for {@link JmxConnectionMetrics#JmxConnectionMetrics} and
     * {@link JmxCollectionMetrics#close()}.
     */
    @Test
    public void test() {
        final JmxSupport mockSupport = createMock(JmxSupport.class);
        final MongoMessageListener mockListener = createMock(MongoMessageListener.class);

        mockSupport.register(anyObject(AbstractMetrics.class),
                eq("connection"), eq("name"), anyInt());

        replay(mockSupport);

        final JmxConnectionMetrics metrics = new JmxConnectionMetrics(
                mockSupport, "name", mockListener);

        verify(mockSupport);

        // And close.

        reset(mockSupport);
        mockSupport.unregister(eq("connection"), eq("name"), anyInt());
        replay(mockSupport);

        metrics.close();

        verify(mockSupport);
    }

}
