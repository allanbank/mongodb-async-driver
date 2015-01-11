/*
 * #%L
 * MetricsMXBean.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.management.MXBean;

/**
 * MetricsMXBean provides the interface for the basic set of metrics collected.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@MXBean(true)
public interface MetricsMXBean {
    /**
     * Returns the average latency for all received messages in millisecond.
     *
     * @return The average latency for all received messages in millisecond.
     */
    public double getAverageLatencyMillis();

    /**
     * Returns the latency for the last message received in nanoseconds.
     *
     * @return The latency for the last message received in nanoseconds.
     */
    public long getLastLatencyNanos();

    /**
     * Returns the number of received bytes.
     *
     * @return The number of received bytes.
     */
    public long getMessageReceivedBytes();

    /**
     * Returns the number of received messages.
     *
     * @return The number of received messages.
     */
    public long getMessageReceivedCount();

    /**
     * Returns the number of sent bytes.
     *
     * @return The number of sent bytes.
     */
    public long getMessageSentBytes();

    /**
     * Returns the number of sent messages.
     *
     * @return The number of sent messages.
     */
    public long getMessageSentCount();

    /**
     * Returns the recent average received latency in millisecond.
     *
     * @return The recent average received latency in millisecond.
     */
    public double getRecentAverageLatencyMillis();

    /**
     * Returns the total latency for all received messages in nanoseconds.
     *
     * @return The total latency for all received messages in nanoseconds.
     */
    public long getTotalLatencyNanos();
}
