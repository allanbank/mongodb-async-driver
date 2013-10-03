/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.message;

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

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.client.connection.Message;

/**
 * PendingMessageTest provides tests for the {@link PendingMessage} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * {@link PendingMessage#PendingMessage(int, Message, Callback)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testPendingMessageIntMessageCallbackOfReply() {
        final Random random = new Random(System.currentTimeMillis());

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();
        final Callback<Reply> mockCallback = createMock(Callback.class);

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
        assertTrue(delta < TimeUnit.MILLISECONDS.toNanos(2));
    }
}
