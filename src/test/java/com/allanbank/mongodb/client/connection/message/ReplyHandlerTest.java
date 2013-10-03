/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.message;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.connection.FutureCallback;
import com.allanbank.mongodb.client.connection.Message;

/**
 * ReplyHandlerTest provides tests for the {@link ReplyHandler} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyHandlerTest {

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, Callback, Executor)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRaiseError() {
        final Executor executor = new CallerExecutor();

        final Message mockMessage = createMock(Message.class);
        final Callback<Reply> mockCallback = createMock(Callback.class);

        final Throwable thrown = new Throwable();

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(thrown, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, Callback, Executor)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRaiseErrorWhenRejected() {
        final Executor executor = new RejectionExecutor();

        final Message mockMessage = createMock(Message.class);
        final Callback<Reply> mockCallback = createMock(Callback.class);

        final Throwable thrown = new Throwable();

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(thrown, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, Callback, Executor)}.
     */
    @Test
    public void testRaiseErrorWithoutCallback() {

        // To verify the executor is not really used.
        final Executor executor = createMock(Executor.class);

        final Throwable thrown = new Throwable();

        replay(executor);
        ReplyHandler.raiseError(thrown, null, executor);
        verify(executor);
    }

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, Callback, Executor)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRaiseErrorWithoutError() {
        final Executor executor = new CallerExecutor();

        final Message mockMessage = createMock(Message.class);
        final Callback<Reply> mockCallback = createMock(Callback.class);

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(null, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link ReplyHandler#reply(Reply, Callback, Executor)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReply() {
        final Executor executor = new CallerExecutor();

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final Callback<Reply> mockCallback = createMock(Callback.class);

        mockCallback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.reply(reply, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link ReplyHandler#reply(Reply, Callback, Executor)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReplyWhenRejected() {
        final Executor executor = new RejectionExecutor();

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final Callback<Reply> mockCallback = createMock(Callback.class);

        mockCallback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.reply(reply, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for {@link ReplyHandler#reply(Reply, Callback, Executor)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testReplyWithFutureCallback() throws InterruptedException,
            ExecutionException {
        final Executor mockEexecutor = createMock(Executor.class);

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final FutureCallback<Reply> callback = new FutureCallback<Reply>();

        replay(mockMessage, mockEexecutor);

        ReplyHandler.reply(reply, callback, mockEexecutor);

        assertSame(reply, callback.get());

        verify(mockMessage, mockEexecutor);
    }

    /**
     * Test method for {@link ReplyHandler#reply(Reply, Callback, Executor)}.
     */
    @Test
    public void testReplyWithoutCallback() {
        // To verify the executor is not really used.
        final Executor executor = createMock(Executor.class);

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        replay(executor);
        ReplyHandler.reply(reply, null, executor);
        verify(executor);
    }

    /**
     * CallerExecutor provides a simple executor.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class CallerExecutor implements Executor {

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to call run on the command.
         * </p>
         */
        @Override
        public void execute(final Runnable command) {
            command.run();
        }
    }

    /**
     * RejectionExecutor provides a simple executor.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class RejectionExecutor implements Executor {

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to call run on the command.
         * </p>
         */
        @Override
        public void execute(final Runnable command) {
            throw new RejectedExecutionException();
        }
    }
}
