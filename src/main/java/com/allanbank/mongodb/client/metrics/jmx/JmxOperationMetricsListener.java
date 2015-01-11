/*
 * #%L
 * JmxOperationMetricsListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.client.metrics.basic.BasicOperationMetricsListener;

/**
 * JmxOperationMetricsListener provides a listener to populate the server
 * metrics.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxOperationMetricsListener
        extends BasicOperationMetricsListener {

    /** The JMX support class. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxOperationMetricsListener.
     *
     * @param support
     *            The JMX support class.
     */
    public JmxOperationMetricsListener(final JmxSupport support) {
        mySupport = support;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to register the JmxOperationMetrics.
     * </p>
     */
    @Override
    protected void addedOperation(final BasicOperationMetrics metrics) {
        if (metrics instanceof JmxOperationMetrics) {
            ((JmxOperationMetrics) metrics).register();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new JmxOperationMetrics.
     * </p>
     */
    @Override
    protected JmxOperationMetrics createMetrics(final String operationName) {
        return new JmxOperationMetrics(mySupport, operationName);
    }
}
