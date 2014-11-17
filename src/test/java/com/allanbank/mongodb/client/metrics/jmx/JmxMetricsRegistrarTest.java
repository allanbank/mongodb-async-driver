/*
 * #%L
 * JmxMetricsRegistrarTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * JmxMetricsRegistrarTest provides tests for the {@link JmxMetricsRegistrar}
 * class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxMetricsRegistrarTest {

    /**
     * Test method for {@link JmxMetricsRegistrar#registerClient()}.
     */
    @Test
    public void testRegisterClient() {
        final JmxMetricsRegistrar registrar = new JmxMetricsRegistrar();

        assertThat(registrar.registerClient(),
                instanceOf(JmxMongoClientMetrics.class));
    }
}
