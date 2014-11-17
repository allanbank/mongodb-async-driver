/*
 * #%L
 * BasicMongoClientMetricsTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.metrics.MongoMessageListener;
import com.allanbank.mongodb.client.metrics.NoOpMongoMessageListener;

/**
 * BasicMongoClientMetricsTest provides tests for the
 * {@link BasicMongoClientMetrics} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BasicMongoClientMetricsTest {

    /**
     * Test method for
     * {@link BasicMongoClientMetrics#connectionClosed(MongoMessageListener)}.
     */
    @Test
    public void testConnectionClosed() {
        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        // Nothing to see, move along.
        metrics.connectionClosed(null);

        metrics.close();
    }

    /**
     * Test method for {@link BasicMongoClientMetrics#newConnection(String)}.
     */
    @Test
    public void testNewConnection() {
        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        assertThat(metrics.newConnection("server1"),
                instanceOf(BasicConnectionMetrics.class));

        metrics.close();
    }

    /**
     * Test method for
     * {@link BasicMongoClientMetrics#receive(String, long, Message, Reply, long)}
     * .
     */
    @Test
    public void testReceive() {
        final Message mockSentMessage = createMock(Message.class);
        final Reply mockReply = createMock(Reply.class);

        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        // One for the DB and one for the collection.
        expect(mockReply.size()).andReturn(202).times(5);
        expect(mockSentMessage.getOperationName()).andReturn("operation");
        expect(mockSentMessage.getDatabaseName()).andReturn("db");
        expect(mockSentMessage.getCollectionName()).andReturn("collection");
        expectLastCall();

        replay(mockSentMessage, mockReply);

        metrics.receive("server1", 0L, mockSentMessage, mockReply,
                TimeUnit.MILLISECONDS.toNanos(1));

        metrics.close();

        verify(mockSentMessage, mockReply);
    }

    /**
     * Test method for
     * {@link BasicMongoClientMetrics#sent(String, long, Message)}.
     */
    @Test
    public void testSent() {
        final Message mockSentMessage = createMock(Message.class);

        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        // One for the DB and one for the collection.
        expect(mockSentMessage.size()).andReturn(101).times(10);
        expect(mockSentMessage.getOperationName()).andReturn("operation")
                .times(2);
        expect(mockSentMessage.getDatabaseName()).andReturn("db").times(2);
        expect(mockSentMessage.getCollectionName()).andReturn("collection")
                .times(2);

        replay(mockSentMessage);

        metrics.sent("server1", 0L, mockSentMessage);
        metrics.sent("server2", 0L, mockSentMessage);

        assertThat(
                metrics.toString(),
                is("Global[sentBytes=202, sentCount=2, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Database[db: sentBytes=202, sentCount=2, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Collection[collection: sentBytes=202, sentCount=2, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Operation[operation: sentBytes=202, sentCount=2, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Server[server1: sentBytes=101, sentCount=1, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n"
                        + "Server[server2: sentBytes=101, sentCount=1, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]"));

        metrics.close();

        verify(mockSentMessage);
    }

    /**
     * Test method for
     * {@link BasicMongoClientMetrics#setMessageListener(MongoMessageListener)}.
     */
    @Test
    public void testSetMessageListener() {
        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        metrics.setMessageListener(null);
        metrics.setMessageListener(createMock(MongoMessageListener.class));
        metrics.setMessageListener(NoOpMongoMessageListener.NO_OP);

        metrics.close();
    }

    /**
     * Test method for {@link BasicMongoClientMetrics#writeTo(PrintWriter)}.
     */
    @Test
    public void testWriteToPrintWriter() {
        final BasicMongoClientMetrics metrics = new BasicMongoClientMetrics();

        assertThat(
                metrics.toString(),
                is("Global[sentBytes=0, sentCount=0, receivedBytes=0, "
                        + "receivedCount=0, lastLatency=0 ms, totalLatency=0 ms]\n\n\n"));

        metrics.close();
    }

}
