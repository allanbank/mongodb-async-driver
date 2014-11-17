/*
 * #%L
 * NoOpMetricsRegistrar.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.client.metrics.MetricsRegistrar;
import com.allanbank.mongodb.client.metrics.MongoClientMetrics;

/**
 * NoOpMetricsRegistrar provides the no-op metrics registrar.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpMetricsRegistrar extends MetricsRegistrar {

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a {@link NoOpMongoClientMetrics}.
     * </p>
     */
    @Override
    public MongoClientMetrics registerClient() {
        return new NoOpMongoClientMetrics();
    }
}
