/*
 * #%L
 * ServerUpdateCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.state;

import static com.allanbank.mongodb.client.connection.CallbackReply.reply;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.gt;
import static org.easymock.EasyMock.lt;
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

        state.updateAverageLatency(and(gt(0L), lt(MILLISECONDS.toNanos(100))));
        expectLastCall();

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

        state.updateAverageLatency(and(gt(0L), lt(MILLISECONDS.toNanos(100))));
        expectLastCall();

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
