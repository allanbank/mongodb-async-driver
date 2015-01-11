/*
 * #%L
 * X509ResponseCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * X509ResponseCallbackTest provides tests for the {@link X509ResponseCallback}
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class X509ResponseCallbackTest {

    /**
     * Test method for {@link X509ResponseCallback#exception(Throwable)}.
     *
     * @throws InterruptedException
     *             On a test failure.
     * @throws TimeoutException
     *             On a test failure.
     */
    @Test
    public void testException() throws InterruptedException, TimeoutException {
        final Throwable injected = new MongoDbException("Injected");

        final FutureCallback<Boolean> future = new FutureCallback<Boolean>();
        final X509ResponseCallback cb = new X509ResponseCallback(future);

        cb.exception(injected);

        try {
            future.get(1, TimeUnit.NANOSECONDS);
            fail("Should have thrown an ExecutionException.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause(), sameInstance(injected));
        }
    }

    /**
     * Test method for {@link X509ResponseCallback#handle(Reply)}.
     *
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws TimeoutException
     *             On a test failure.
     */
    @Test
    public void testHandleDone() throws ExecutionException,
            InterruptedException, TimeoutException {
        final Document doc = BuilderFactory.start().add("ok", 1).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        final FutureCallback<Boolean> future = new FutureCallback<Boolean>();
        final X509ResponseCallback cb = new X509ResponseCallback(future);

        cb.handle(reply);

        assertThat(future.get(1, TimeUnit.NANOSECONDS), is(true));
    }

    /**
     * Test method for {@link X509ResponseCallback#handle(Reply)}.
     *
     * @throws InterruptedException
     *             On a test failure.
     * @throws TimeoutException
     *             On a test failure.
     */
    @Test
    public void testHandleFailed() throws InterruptedException,
            TimeoutException {
        final Document doc = BuilderFactory.start().add("ok", 0)
                .add("err", "Testing.").build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        final FutureCallback<Boolean> future = new FutureCallback<Boolean>();
        final X509ResponseCallback cb = new X509ResponseCallback(future);

        cb.handle(reply);

        try {
            future.get(1, TimeUnit.NANOSECONDS);
            fail("Should have thrown an ExecutionException.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause().getMessage(), is("Testing."));
        }
    }

    /**
     * Test method for {@link X509ResponseCallback#isLightWeight()}.
     */
    @Test
    public void testIsLightWeight() {
        final FutureCallback<Boolean> future = new FutureCallback<Boolean>();
        final X509ResponseCallback cb = new X509ResponseCallback(future);

        assertThat(cb.isLightWeight(), is(true));
    }
}
