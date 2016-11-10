/*
 * #%L
 * PendingMessageTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.message;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;

/**
 * PendingMessageTest provides tests for the {@link PendingMessage} class.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PendingMessageTest {

    /**
     * Test method for {@link PendingMessage#PendingMessage(int, Message)}.
     */
    @Test
    public void testPendingMessageIntMessage() {
        final Random random = new Random(System.currentTimeMillis());

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();

        replay(mockMessage);

        final PendingMessage pm = new PendingMessage(id, mockMessage);

        assertEquals(id, pm.getMessageId());
        assertSame(mockMessage, pm.getMessage());
        assertNull(pm.getReplyCallback());

        verify(mockMessage);
    }

    /**
     * Test method for
     * {@link PendingMessage#PendingMessage(int, Message, ReplyCallback)}.
     */
    @Test
    public void testPendingMessageIntMessageCallbackOfReply() {
        final Random random = new Random(System.currentTimeMillis());

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        replay(mockMessage, mockCallback);

        final PendingMessage pm = new PendingMessage(id, mockMessage,
                mockCallback);

        assertEquals(id, pm.getMessageId());
        assertSame(mockMessage, pm.getMessage());
        assertSame(mockCallback, pm.getReplyCallback());

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link PendingMessage#timestampNow()} and
     * {@link PendingMessage#latency()}.
     *
     * @throws InterruptedException
     *             If the test fails to sleep.
     */
    @Test
    public void testTimestampNow() throws InterruptedException {
        final PendingMessage pm = new PendingMessage();

        assertEquals(0L, pm.latency());

        pm.timestampNow();

        TimeUnit.NANOSECONDS.sleep(100);

        final long delta = pm.latency();
        assertTrue(0 < delta);
        assertTrue("delta should be less then " + TimeUnit.MILLISECONDS.toNanos(2) + " but was " + delta, delta < TimeUnit.MILLISECONDS.toNanos(3));
    }
}
