/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.state;

import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * ServerUpdateCallbackTest provides tests for the {@link ServerUpdateCallback}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerUpdateCallbackTest {

    /**
     * Test method for {@link ServerUpdateCallback#callback} .
     */
    @Test
    public void testCallbackWithDocument() {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("ismaster", false);
        builder.add("secondary", true);

        final Server state = createMock(Server.class);

        state.update(builder.asDocument());
        expectLastCall();

        replay(state);

        final ServerUpdateCallback callback = new ServerUpdateCallback(state);
        callback.callback(reply(builder));

        verify(state);

    }

    /**
     * Test method for {@link ServerUpdateCallback#callback} .
     */
    @Test
    public void testCallbackWithoutDocument() {

        final Server state = createMock(Server.class);

        replay(state);

        final ServerUpdateCallback callback = new ServerUpdateCallback(state);
        callback.callback(reply());

        verify(state);
    }

    /**
     * Test method for
     * {@link ServerUpdateCallback#exception(java.lang.Throwable)} .
     */
    @Test
    public void testException() {
        final Server state = createMock(Server.class);

        state.requestFailed();
        expectLastCall();

        replay(state);

        final ServerUpdateCallback callback = new ServerUpdateCallback(state);
        callback.exception(new MongoDbException());

        verify(state);
    }

}
