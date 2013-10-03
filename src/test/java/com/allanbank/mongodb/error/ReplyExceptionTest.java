/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.connection.Message;
import com.allanbank.mongodb.client.connection.message.Reply;

/**
 * ReplyExceptionTest provides tests for the {@link ReplyException}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyExceptionTest {

    /**
     * Test method for
     * {@link ReplyException#ReplyException(int, int, String, Message, Reply)}.
     */
    @Test
    public void testReplyExceptionIntIntStringMessageReply() {
        final Message mock = EasyMock.createMock(Message.class);
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);

        final ReplyException re = new ReplyException(1234, 12345, "foo", mock,
                r);
        assertSame(r, re.getReply());
        assertEquals("foo", re.getMessage());
        assertNull(re.getCause());
        assertEquals(12345, re.getErrorNumber());
        assertEquals(1234, re.getOkValue());
        assertSame(mock, re.getRequest());
    }

    /**
     * Test method for
     * {@link ReplyException#ReplyException(int, int, String, Reply)}.
     */
    @Test
    public void testReplyExceptionIntIntStringReply() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);

        final ReplyException re = new ReplyException(1234, 12345, "foo", r);
        assertSame(r, re.getReply());
        assertEquals("foo", re.getMessage());
        assertNull(re.getCause());
        assertEquals(12345, re.getErrorNumber());
        assertEquals(1234, re.getOkValue());
        assertNull(re.getRequest());
    }

    /**
     * Test method for {@link ReplyException#ReplyException(Reply)}.
     */
    @Test
    public void testReplyExceptionReply() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);

        final ReplyException re = new ReplyException(r);
        assertSame(r, re.getReply());
        assertNull(re.getMessage());
        assertNull(re.getCause());
        assertEquals(-1, re.getErrorNumber());
        assertEquals(-1, re.getOkValue());
        assertNull(re.getRequest());
    }

    /**
     * Test method for {@link ReplyException#ReplyException(Reply, String)}.
     */
    @Test
    public void testReplyExceptionReplyString() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);

        final ReplyException re = new ReplyException(r, "foo");
        assertSame(r, re.getReply());
        assertEquals("foo", re.getMessage());
        assertNull(re.getCause());
        assertEquals(-1, re.getErrorNumber());
        assertEquals(-1, re.getOkValue());
        assertNull(re.getRequest());
    }

    /**
     * Test method for {@link ReplyException#ReplyException(Reply, Throwable)}.
     */
    @Test
    public void testReplyExceptionReplyThrowable() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);
        final Throwable t = new Throwable();

        final ReplyException re = new ReplyException(r, t);
        assertSame(r, re.getReply());
        assertSame(t, re.getCause());
        assertNull(re.getMessage());
        assertEquals(-1, re.getErrorNumber());
        assertEquals(-1, re.getOkValue());
        assertNull(re.getRequest());
    }
}
