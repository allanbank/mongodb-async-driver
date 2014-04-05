/*
 * Copyright 2012-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * ReplyHandlerTest provides tests for the {@link ReplyHandler} class.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyHandlerTest {

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

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, ReplyCallback, Executor)}.
     */
    @Test
    public void testRaiseError() {
        final Executor executor = new CallerExecutor();

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        final Throwable thrown = new Throwable();

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(thrown, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, ReplyCallback, Executor)}.
     */
    @Test
    public void testRaiseErrorWhenRejected() {
        final Executor executor = new RejectionExecutor();

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        final Throwable thrown = new Throwable();

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(thrown, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#raiseError(Throwable, ReplyCallback, Executor)}.
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
     * {@link ReplyHandler#raiseError(Throwable, ReplyCallback, Executor)}.
     */
    @Test
    public void testRaiseErrorWithoutError() {
        final Executor executor = new CallerExecutor();

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        replay(mockMessage, mockCallback);

        ReplyHandler.raiseError(null, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
     */
    @Test
    public void testReply() {
        final Executor executor = new CallerExecutor();

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        expect(mockCallback.isLightWeight()).andReturn(true);

        mockCallback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.reply(reply, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
     */
    @Test
    public void testReplyWhenRejected() {
        final Executor executor = new RejectionExecutor();

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback mockCallback = createMock(ReplyCallback.class);

        expect(mockCallback.isLightWeight()).andReturn(true);

        mockCallback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockCallback);

        ReplyHandler.reply(reply, mockCallback, executor);

        verify(mockMessage, mockCallback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
     *
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testReplyWithLightWeightCallback() throws InterruptedException,
            ExecutionException {
        final Executor mockExecutor = createMock(Executor.class);

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback callback = createMock(ReplyCallback.class);

        expect(callback.isLightWeight()).andReturn(true);
        callback.callback(reply);
        expectLastCall();

        replay(mockMessage, mockExecutor, callback);

        ReplyHandler.reply(reply, callback, mockExecutor);

        verify(mockMessage, mockExecutor, callback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
     *
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testReplyWithNonLightWeightCallback()
            throws InterruptedException, ExecutionException {
        final Executor mockExecutor = createMock(Executor.class);

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback callback = createMock(ReplyCallback.class);

        expect(callback.isLightWeight()).andReturn(false);

        mockExecutor.execute(EasyMock.anyObject(Runnable.class));
        expectLastCall();

        replay(mockMessage, mockExecutor, callback);

        ReplyHandler.reply(reply, callback, mockExecutor);

        verify(mockMessage, mockExecutor, callback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
     *
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testReplyWithNonLightWeightCallbackNoExecutor()
            throws InterruptedException, ExecutionException {
        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Message mockMessage = createMock(Message.class);
        final ReplyCallback callback = createMock(ReplyCallback.class);

        expect(callback.isLightWeight()).andReturn(false);

        callback.callback(reply);
        expectLastCall();

        replay(mockMessage, callback);

        ReplyHandler.reply(reply, callback, null);

        verify(mockMessage, callback);
    }

    /**
     * Test method for
     * {@link ReplyHandler#reply(Reply, ReplyCallback, Executor)}.
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
}
