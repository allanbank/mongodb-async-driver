/*
 * #%L
 * BasicConnectionMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;

/**
 * BasicConnectionMetricsTest provides tests for the
 * {@link BasicConnectionMetrics} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicConnectionMetricsTest {

    /**
     * Test method for
     * {@link BasicConnectionMetrics#receive(String, long, Message, Reply, long)}
     * .
     */
    @Test
    public void testReceive() {
        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);
        final MongoMessageListener mockListener = createMock(MongoMessageListener.class);

        final BasicConnectionMetrics metrics = new BasicConnectionMetrics(
                mockListener);

        expect(mockReply.size()).andReturn(202);
        mockListener.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));
        expectLastCall();

        replay(mockSentMessage, mockReply, mockListener);

        metrics.receive(null, 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        metrics.close();

        verify(mockSentMessage, mockReply, mockListener);
    }

    /**
     * Test method for
     * {@link BasicConnectionMetrics#sent(String, long, Message)} .
     */
    @Test
    public void testSent() {
        final Message mockSentMessage = createMock(Message.class);
        final MongoMessageListener mockListener = createMock(MongoMessageListener.class);

        final BasicConnectionMetrics metrics = new BasicConnectionMetrics(
                mockListener);

        expect(mockSentMessage.size()).andReturn(101);
        mockListener.sent(null, 0L, mockSentMessage);
        expectLastCall();

        replay(mockSentMessage, mockListener);

        metrics.sent(null, 0L, mockSentMessage);

        metrics.close();

        verify(mockSentMessage, mockListener);
    }

    /**
     * Test method for {@link BasicConnectionMetrics#writeTo(PrintWriter)}.
     */
    @Test
    public void testWriteToPrintWriter() {
        final StringWriter sink = new StringWriter();
        final PrintWriter writer = new PrintWriter(sink);

        final BasicConnectionMetrics metrics = new BasicConnectionMetrics(null);

        metrics.writeTo(writer);
        assertThat(
                sink.toString(),
                is("Connection["
                        + System.identityHashCode(metrics)
                        + ": sentBytes=0, sentCount=0, receivedBytes=0, receivedCount=0, "
                        + "lastLatency=0 ms, totalLatency=0 ms]"));

        // For Closeable.
        metrics.close();
    }
}
