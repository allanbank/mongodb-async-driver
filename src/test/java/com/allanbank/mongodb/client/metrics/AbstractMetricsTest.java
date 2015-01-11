/*
 * #%L
 * AbstractMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * AbstractMetricsTest provides tests for the {@link AbstractMetrics} base
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AbstractMetricsTest {

    /**
     * Test method for {@link AbstractMetrics#close()}.
     */
    @Test
    public void test() {

        final TestMetrics metrics = new TestMetrics();

        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);

        expect(mockSentMessage.size()).andReturn(101).times(2);
        expect(mockReply.size()).andReturn(202).times(2);

        replay(mockSentMessage, mockReply);

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        assertThat(metrics.getAverageLatencyMillis(), both(greaterThan(0.9))
                .and(lessThan(1.1)));
        assertThat(metrics.getLastLatencyNanos(),
                is(TimeUnit.MILLISECONDS.toNanos(1)));
        assertThat(metrics.getMessageReceivedBytes(), is(404L));
        assertThat(metrics.getMessageReceivedCount(), is(2L));
        assertThat(metrics.getMessageSentBytes(), is(202L));
        assertThat(metrics.getMessageSentCount(), is(2L));
        assertThat(metrics.getRecentAverageLatencyMillis(),
                both(greaterThan(0.9)).and(lessThan(1.1)));
        assertThat(metrics.getTotalLatencyNanos(),
                is(TimeUnit.MILLISECONDS.toNanos(1) << 1));

        metrics.close();

        assertThat(Double.isNaN(metrics.getAverageLatencyMillis()), is(true));
        assertThat(metrics.getLastLatencyNanos(), is(0L));
        assertThat(metrics.getMessageReceivedBytes(), is(0L));
        assertThat(metrics.getMessageReceivedCount(), is(0L));
        assertThat(metrics.getMessageSentBytes(), is(0L));
        assertThat(metrics.getMessageSentCount(), is(0L));
        assertThat(Double.isNaN(metrics.getRecentAverageLatencyMillis()),
                is(true));
        assertThat(metrics.getTotalLatencyNanos(), is(0L));

        verify(mockSentMessage, mockReply);
    }

    /**
     * Test method for {@link AbstractMetrics#toString()}.
     */
    @Test
    public void testToString() {
        final TestMetrics metrics = new TestMetrics();

        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);

        expect(mockSentMessage.size()).andReturn(101).times(2);
        expect(mockReply.size()).andReturn(202).times(2);

        replay(mockSentMessage, mockReply);

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        assertThat(
                metrics.toString(),
                is("test[sentBytes=202, sentCount=2, receivedBytes=404, receivedCount=2, "
                        + "lastLatency=1 ms, totalLatency=2 ms, recentAverageLatency=1 ms, "
                        + "averageLatency=1 ms]"));

        metrics.close();

        verify(mockSentMessage, mockReply);
    }

    /**
     * Test method for
     * {@link AbstractMetrics#writeTo(PrintWriter, String, String)}.
     */
    @Test
    public void testWriteToPrintWriterStringString() {
        final TestMetrics metrics = new TestMetrics();

        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);

        expect(mockSentMessage.size()).andReturn(101).times(2);
        expect(mockReply.size()).andReturn(202).times(2);

        replay(mockSentMessage, mockReply);

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        metrics.sent(null, 0L, mockSentMessage);
        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        final StringWriter writer = new StringWriter();
        final PrintWriter pWriter = new PrintWriter(writer);

        metrics.writeTo(pWriter, "foo", "bar");

        assertThat(
                writer.toString(),
                is("foo[bar: sentBytes=202, sentCount=2, receivedBytes=404, "
                        + "receivedCount=2, lastLatency=1 ms, totalLatency=2 ms, "
                        + "recentAverageLatency=1 ms, averageLatency=1 ms]"));

        metrics.close();

        verify(mockSentMessage, mockReply);
    }

    /**
     * TestMetrics provides a simple example using the {@link AbstractMetrics}
     * class.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class TestMetrics
            extends AbstractMetrics {
        /**
         * {@inheritDoc}
         */
        @Override
        public void writeTo(final PrintWriter writer) {
            writeTo(writer, "test", null);
        }
    }
}
