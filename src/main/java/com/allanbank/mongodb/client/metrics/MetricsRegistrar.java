/*
 * #%L
 * MetricsRegistrar.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics;

import com.allanbank.mongodb.client.metrics.basic.BasicMetricsRegistrar;
import com.allanbank.mongodb.client.metrics.jmx.JmxMetricsRegistrar;
import com.allanbank.mongodb.client.metrics.noop.NoOpMetricsRegistrar;

/**
 * MetricsRegistrar provides the ability to register metrics based on the
 * available metrics exposing methods.
 * <p>
 * Normally this will be done via JMX but on Android systems JMX is not
 * available and this will fall back to a periodic log message with the
 * accumulated metrics.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class MetricsRegistrar {

    /**
     * Creates a metrics registrar.
     *
     * @param metricsOn
     *            If true then the driver should collect metrics.
     * @return The available metrics registrar.
     */
    public static final MetricsRegistrar createRegistrar(final boolean metricsOn) {
        if (metricsOn) {
            try {
                return new JmxMetricsRegistrar();
            }
            catch (final Throwable anyError) {
                return new BasicMetricsRegistrar();
            }
        }
        return new NoOpMetricsRegistrar();
    }

    /**
     * Creates a new MetricsRegistrar.
     */
    public MetricsRegistrar() {
        super();
    }

    /**
     * Registers a new client with the registrar and returns the
     * {@link MongoClientMetrics} for updating and managing the exposed metrics.
     *
     * @return The {@link MongoClientMetrics} for updating and managing exposed
     *         metrics.
     */
    public abstract MongoClientMetrics registerClient();
}
