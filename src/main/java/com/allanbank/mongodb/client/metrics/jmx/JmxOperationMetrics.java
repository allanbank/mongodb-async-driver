/*
 * #%L
 * JmxOperationMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.client.metrics.basic.BasicOperationMetrics;

/**
 * JmxOperationMetrics provides metrics for a single operation type.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxOperationMetrics
        extends BasicOperationMetrics {

    /** The JMX support class. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxOperationMetrics.
     *
     * @param support
     *            JMX support class.
     * @param operationName
     *            The name of operation type.
     */
    public JmxOperationMetrics(final JmxSupport support,
            final String operationName) {
        super(operationName);

        mySupport = support;
    }

    /**
     * Removes the operation from JMX.
     */
    @Override
    public void close() {
        super.close();
        mySupport.unregister("operation", getName());
    }

    /**
     * Registers with the MBean server.
     */
    public void register() {
        mySupport.register(this, "operation", getName());
    }
}