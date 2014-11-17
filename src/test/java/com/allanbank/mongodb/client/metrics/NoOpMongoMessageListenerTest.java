/*
 * #%L
 * NoOpMongoMessageListenerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics;

import org.junit.Test;

/**
 * NoOpMongoMessageListenerTest provides tests for the
 * {@link NoOpMongoMessageListener} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpMongoMessageListenerTest {

    /**
     * Test method for {@link NoOpMongoMessageListener#close()}.
     */
    @Test
    public void testClose() {
        final NoOpMongoMessageListener listener = NoOpMongoMessageListener.NO_OP;

        // Nothing to see here. Move along.
        listener.close();
    }

    /**
     * Test method for {@link NoOpMongoMessageListener#receive}.
     */
    @Test
    public void testReceive() {
        final NoOpMongoMessageListener listener = NoOpMongoMessageListener.NO_OP;

        // Nothing to see here. Move along.
        listener.receive(null, 0L, null, null, 0L);
    }

    /**
     * Test method for {@link NoOpMongoMessageListener#sent}.
     */
    @Test
    public void testSent() {
        final NoOpMongoMessageListener listener = NoOpMongoMessageListener.NO_OP;

        // Nothing to see here. Move along.
        listener.sent(null, 0L, null);
    }
}
