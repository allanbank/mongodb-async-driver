/*
 * #%L
 * MetricsMXBeanProxy.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.client.metrics.AbstractMetrics;

/**
 * JmxMetricsProxy provides the MXBean that is registered with JMX and delegates
 * to a non-JMX tainted class.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MetricsMXBeanProxy implements MetricsMXBean {

    /** The proxied set of metrics to delegate to. */
    private final AbstractMetrics myDelegate;

    /**
     * Creates a new JmxMetricsProxy.
     * 
     * @param delegate
     *            The proxied set of metrics to delegate to.
     */
    public MetricsMXBeanProxy(final AbstractMetrics delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public double getAverageLatencyMillis() {
        return myDelegate.getAverageLatencyMillis();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getLastLatencyNanos() {
        return myDelegate.getLastLatencyNanos();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getMessageReceivedBytes() {
        return myDelegate.getMessageReceivedBytes();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getMessageReceivedCount() {
        return myDelegate.getMessageReceivedCount();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getMessageSentBytes() {
        return myDelegate.getMessageSentBytes();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getMessageSentCount() {
        return myDelegate.getMessageSentCount();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public double getRecentAverageLatencyMillis() {
        return myDelegate.getRecentAverageLatencyMillis();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the same method on the delegate.
     * </p>
     */
    @Override
    public long getTotalLatencyNanos() {
        return myDelegate.getTotalLatencyNanos();
    }

}
