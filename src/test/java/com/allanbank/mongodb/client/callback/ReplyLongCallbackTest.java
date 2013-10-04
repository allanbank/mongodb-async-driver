/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * ReplyLongCallbackTest provides tests for the {@link ReplyLongCallback}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyLongCallbackTest {

    /**
     * Test method for {@link ReplyLongCallback#asError(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorReply() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addLong("ok", -23).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertEquals(-23, error.getOkValue());

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#asError(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorReplyNoN() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addLong("ok", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertTrue(error.getMessage().contains("Missing"));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#asError(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorReplyWithN() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addLong("ok", 1).addLong("n", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback("n",
                mockCallback);
        assertNull(callback.asError(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#asError(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorReplyWithToManyDocs() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .addLong("ok", 1).addLong("n", 44).build(), BuilderFactory
                .start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback("n",
                mockCallback);
        assertNull(callback.asError(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReply() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addLong("ok", 1).addLong("n", 44).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback("n",
                mockCallback);
        assertEquals(Long.valueOf(44), callback.convert(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyNonNumeric() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addLong("ok", 1).addString("n", "a").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback("n",
                mockCallback);
        assertEquals(Long.valueOf(-1), callback.convert(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyLongCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyToManyDocs() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .addLong("ok", 1).addLong("n", 44).build(), BuilderFactory
                .start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Long> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyLongCallback callback = new ReplyLongCallback("n",
                mockCallback);
        assertEquals(Long.valueOf(-1), callback.convert(reply));

        verify(mockCallback);
    }

}
