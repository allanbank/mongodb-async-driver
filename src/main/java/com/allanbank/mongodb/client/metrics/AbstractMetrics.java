/*
 * #%L
 * AbstractMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * AbstractMetrics provides the ability to accumulate the basic send/receive
 * metrics.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractMetrics implements MongoMessageListener,
        Closeable {

    /** The decay rate for the exponential average for the latency. */
    private static final double DECAY_ALPHA;

    /** The decay period (number of samples) for the average latency. */
    private static final double DECAY_SAMPLES = 1000.0D;

    /** The number of nano-seconds per milli-second. */
    private static final double NANOS_PER_MILLI = TimeUnit.MILLISECONDS
            .toNanos(1);

    /** The updater for all of the message received counts. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourLastLatencyNanosUpdater;

    /** The updater for all of the message received bytes. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourMessageReceivedBytesUpdater;

    /** The updater for all of the message received counts. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourMessageReceivedUpdater;

    /** The updater for all of the message sent bytes. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourMessageSentBytesUpdater;

    /** The updater for all of the message sent counts. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourMessageSentUpdater;

    /** The updater for all of the message received counts. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourRecentAverageLatencyMillisUpdater;

    /** The updater for all of the message received counts. */
    private final static AtomicLongFieldUpdater<AbstractMetrics> ourTotalLatencyNanosUpdater;

    static {
        DECAY_ALPHA = (2.0D / (DECAY_SAMPLES + 1));

        ourLastLatencyNanosUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myLastLatencyNanos");
        ourMessageReceivedBytesUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myMessageReceivedBytes");
        ourMessageReceivedUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myMessageReceivedCount");
        ourMessageSentBytesUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myMessageSentBytes");
        ourMessageSentUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myMessageSentCount");
        ourRecentAverageLatencyMillisUpdater = AtomicLongFieldUpdater
                .newUpdater(AbstractMetrics.class,
                        "myRecentAverageLatencyMillis");
        ourTotalLatencyNanosUpdater = AtomicLongFieldUpdater.newUpdater(
                AbstractMetrics.class, "myTotalLatencyNanos");
    }

    /** The latency for the last message received. */
    private volatile long myLastLatencyNanos;

    /** The number of received bytes. */
    private volatile long myMessageReceivedBytes;

    /** The number of received messages. */
    private volatile long myMessageReceivedCount;

    /** The number of sent bytes. */
    private volatile long myMessageSentBytes;

    /** The number of sent messages. */
    private volatile long myMessageSentCount;

    /**
     * Tracks the recent average received latency. This is updated periodically
     * using an exponential moving average.
     * <p>
     * The double value is stored as a long via
     * {@link Double#doubleToLongBits(double)}.
     * </p>
     */
    private volatile long myRecentAverageLatencyMillis;

    /** The total latency for all received messages. */
    private volatile long myTotalLatencyNanos;

    /**
     * Creates a new AbstractMetrics.
     */
    public AbstractMetrics() {
        reset();
    }

    /**
     * Notification that the metrics are no longer needed. Resets all of the
     * metrics values to initial values.
     */
    @Override
    public void close() {
        reset();
    }

    /**
     * Returns the average latency for all received messages in nanoseconds.
     * 
     * @return The average latency for all received messages in nanoseconds.
     */
    public double getAverageLatencyMillis() {
        final double total = getTotalLatencyNanos() / NANOS_PER_MILLI;
        final double count = getMessageReceivedCount();

        return total / count;
    }

    /**
     * Returns the latency for the last message received in nanoseconds.
     * 
     * @return The latency for the last message received in nanoseconds.
     */
    public long getLastLatencyNanos() {
        return myLastLatencyNanos;
    }

    /**
     * Returns the number of received bytes.
     * 
     * @return The number of received bytes.
     */
    public long getMessageReceivedBytes() {
        return myMessageReceivedBytes;
    }

    /**
     * Returns the number of received messages.
     * 
     * @return The number of received messages.
     */
    public long getMessageReceivedCount() {
        return myMessageReceivedCount;
    }

    /**
     * Returns the number of sent bytes.
     * 
     * @return The number of sent bytes.
     */
    public long getMessageSentBytes() {
        return myMessageSentBytes;
    }

    /**
     * Returns the number of sent messages.
     * 
     * @return The number of sent messages.
     */
    public long getMessageSentCount() {
        return myMessageSentCount;
    }

    /**
     * Returns the recent average received latency in nanoseconds.
     * 
     * @return The recent average received latency in nanoseconds.
     */
    public double getRecentAverageLatencyMillis() {
        return Double.longBitsToDouble(myRecentAverageLatencyMillis);
    }

    /**
     * Returns the total latency for all received messages in nanoseconds.
     * 
     * @return The total latency for all received messages in nanoseconds.
     */
    public long getTotalLatencyNanos() {
        return myTotalLatencyNanos;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to accumulate the receive metrics.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        ourMessageReceivedUpdater.incrementAndGet(this);
        ourMessageReceivedBytesUpdater.addAndGet(this, reply.size());
        ourTotalLatencyNanosUpdater.addAndGet(this, latencyNanos);
        ourLastLatencyNanosUpdater.lazySet(this, latencyNanos);

        // Compute a moving average for the recent latency.
        final double latency = latencyNanos / NANOS_PER_MILLI;
        final double oldAverage = getRecentAverageLatencyMillis();

        long newValue;
        if (Double.isNaN(oldAverage)) {
            newValue = Double.doubleToLongBits(latency);
        }
        else {
            final double newLatency = (DECAY_ALPHA * latency)
                    + ((1.0D - DECAY_ALPHA) * oldAverage);

            newValue = Double.doubleToLongBits(newLatency);
        }

        ourRecentAverageLatencyMillisUpdater.set(this, newValue);
    }

    /**
     * Resets the metrics counts.
     */
    public void reset() {
        myLastLatencyNanos = 0L;
        myMessageReceivedBytes = 0L;
        myMessageReceivedCount = 0L;
        myMessageSentBytes = 0L;
        myMessageSentCount = 0L;
        myRecentAverageLatencyMillis = Double.doubleToRawLongBits(Double.NaN);
        myTotalLatencyNanos = 0L;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to accumulate the send metrics.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        ourMessageSentUpdater.incrementAndGet(this);
        ourMessageSentBytesUpdater.addAndGet(this, sent.size());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to delegate to the {@link #writeTo(PrintWriter)} method.
     * </p>
     */
    @Override
    public String toString() {
        final StringWriter sink = new StringWriter();
        final PrintWriter writer = new PrintWriter(sink);

        writeTo(writer);

        return sink.toString();
    }

    /**
     * Writes a human readable form of the metrics.
     * 
     * @param writer
     *            The writer to write to.
     */
    public abstract void writeTo(PrintWriter writer);

    /**
     * Creates a human readable form of the metrics with the specified name.
     * 
     * @param writer
     *            The writer to write the string to.
     * @param type
     *            The type of metrics.
     * @param name
     *            The name of the metrics.
     */
    protected void writeTo(final PrintWriter writer, final String type,
            final String name) {

        if (type != null) {
            writer.append(type).append('[');
        }
        if (name != null) {
            writer.append(name).append(": ");
        }

        final NumberFormat intFormat = NumberFormat.getIntegerInstance();
        final NumberFormat doubleFormat = NumberFormat.getNumberInstance();

        writer.append("sentBytes=");
        writer.append(intFormat.format(myMessageSentBytes));

        writer.append(", sentCount=");
        writer.append(intFormat.format(myMessageSentCount));

        writer.append(", receivedBytes=");
        writer.append(intFormat.format(myMessageReceivedBytes));

        writer.append(", receivedCount=");
        writer.append(intFormat.format(myMessageReceivedCount));

        writer.append(", lastLatency=");
        writer.append(doubleFormat.format(myLastLatencyNanos / NANOS_PER_MILLI));

        writer.append(" ms, totalLatency=");
        writer.append(doubleFormat
                .format(myTotalLatencyNanos / NANOS_PER_MILLI));

        double average = getRecentAverageLatencyMillis();
        if (!Double.isNaN(average)) {
            writer.append(" ms, recentAverageLatency=");
            writer.append(doubleFormat.format(average));
        }

        average = getAverageLatencyMillis();
        if (!Double.isNaN(average)) {
            writer.append(" ms, averageLatency=");
            writer.append(doubleFormat.format(average));
        }

        writer.append(" ms");

        if (type != null) {
            writer.append(']');
        }
    }

}
