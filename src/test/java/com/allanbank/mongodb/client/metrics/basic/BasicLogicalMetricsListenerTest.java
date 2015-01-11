/*
 * #%L
 * BasicLogicalMetricsListenerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.metrics.basic;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * BasicLogicalMetricsListenerTest provides tests for the
 * {@link BasicLogicalMetricsListener} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicLogicalMetricsListenerTest {

    /**
     * Test method for
     * {@link BasicLogicalMetricsListener#receive(String, long, Message, Reply, long)}
     * .
     */
    @Test
    public void testReceive() {
        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);

        final BasicLogicalMetricsListener metrics = new BasicLogicalMetricsListener();

        // One for the DB and one for the collection.
        expect(mockReply.size()).andReturn(202).times(2);
        expect(mockSentMessage.getDatabaseName()).andReturn("db");
        expect(mockSentMessage.getCollectionName()).andReturn("collection");
        expectLastCall();

        replay(mockSentMessage, mockReply);

        assertThat(metrics.getDatabaseMetrics(), not(hasKey("db")));

        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        assertThat(metrics.getDatabaseMetrics(), hasKey("db"));

        metrics.close();

        verify(mockSentMessage, mockReply);
    }

    /**
     * Test method for
     * {@link BasicLogicalMetricsListener#sent(String, long, Message)} .
     */
    @Test
    public void testSent() {
        final Message mockSentMessage = createMock(Message.class);

        final BasicLogicalMetricsListener metrics = new BasicLogicalMetricsListener();

        // One for the DB and one for the collection.
        expect(mockSentMessage.size()).andReturn(101).times(4);
        expect(mockSentMessage.getDatabaseName()).andReturn("db").times(2);
        expect(mockSentMessage.getCollectionName()).andReturn("collection")
                .times(2);

        replay(mockSentMessage);

        assertThat(metrics.getDatabaseMetrics(), not(hasKey("db")));

        metrics.sent(null, 0L, mockSentMessage);
        metrics.sent(null, 0L, mockSentMessage);

        assertThat(metrics.getDatabaseMetrics(), hasKey("db"));

        assertThat(
                metrics.toString(),
                is("Database[db: sentBytes=202, sentCount=2, receivedBytes=0, receivedCount=0, "
                        + "lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Collection[collection: sentBytes=202, sentCount=2, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]"));

        metrics.close();

        verify(mockSentMessage);
    }

    /**
     * Test method for {@link BasicLogicalMetricsListener#toString()}.
     */
    @Test
    public void testToString() {
        final BasicLogicalMetricsListener metrics = new BasicLogicalMetricsListener();
        assertThat(metrics.toString(), is(""));
    }

    /**
     * Test method for
     * {@link BasicLogicalMetricsListener#writeTo(java.io.PrintWriter)}.
     */
    @Test
    public void testWriteTo() {
        final StringWriter sink = new StringWriter();
        final PrintWriter writer = new PrintWriter(sink);

        final BasicLogicalMetricsListener metrics = new BasicLogicalMetricsListener();

        metrics.writeTo(writer);
        assertThat(sink.toString(), is(""));

        // For Closeable.
        metrics.close();
    }

}
