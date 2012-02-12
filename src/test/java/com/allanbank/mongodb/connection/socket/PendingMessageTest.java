/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * PendingMessageTest provides tests for the {@link PendingMessage} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
     * Test method for {@link PendingMessage#raiseError(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRaiseError() {
        final Random random = new Random(System.currentTimeMillis());

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();
        final Callback<Reply> mockCallback = createMock(Callback.class);

        final Throwable thrown = new Throwable();

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockMessage, mockCallback);

        final PendingMessage pm = new PendingMessage(id, mockMessage,
                mockCallback);

        assertEquals(id, pm.getMessageId());
        assertSame(mockMessage, pm.getMessage());
        assertSame(mockCallback, pm.getReplyCallback());

        pm.raiseError(thrown);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link PendingMessage#raiseError(Throwable)}.
     */
    @Test
    public void testRaiseErrorWithoutCallback() {
        final Random random = new Random(System.currentTimeMillis());

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();

        final Throwable thrown = new Throwable();

        replay(mockMessage);

        final PendingMessage pm = new PendingMessage(id, mockMessage);
        pm.raiseError(thrown);

        verify(mockMessage);
    }

    /**
     * Test method for {@link PendingMessage#reply(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReply() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();
        final Callback<Reply> mockCallback = createMock(Callback.class);

        mockCallback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockCallback);

        final PendingMessage pm = new PendingMessage(id, mockMessage,
                mockCallback);

        assertEquals(id, pm.getMessageId());
        assertSame(mockMessage, pm.getMessage());
        assertSame(mockCallback, pm.getReplyCallback());

        pm.reply(reply);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link PendingMessage#reply(Reply)}.
     */
    @Test
    public void testReplyWithoutCallback() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final int id = random.nextInt();

        replay(mockMessage);

        final PendingMessage pm = new PendingMessage(id, mockMessage);

        pm.reply(reply);

        verify(mockMessage);
    }
}
