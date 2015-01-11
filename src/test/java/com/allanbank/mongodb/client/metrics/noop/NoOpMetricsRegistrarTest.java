/*
 * #%L
 * NoOpMetricsRegistrarTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * NoOpMetricsRegistrarTest provides provides tests for the
 * {@link NoOpMetricsRegistrar} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpMetricsRegistrarTest {

    /**
     * Test method for {@link NoOpMetricsRegistrar#registerClient()}.
     */
    @Test
    public void testRegisterClient() {
        final NoOpMetricsRegistrar registrar = new NoOpMetricsRegistrar();

        assertThat(registrar.registerClient(),
                instanceOf(NoOpMongoClientMetrics.class));
    }
}
