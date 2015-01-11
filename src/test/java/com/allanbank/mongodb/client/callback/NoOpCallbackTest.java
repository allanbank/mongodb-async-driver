/*
 * #%L
 * NoOpCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.callback;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * NoOpCallbackTest provides tests for the {@link NoOpCallback} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpCallbackTest {

    /**
     * Test method for {@link NoOpCallback#callback}.
     */
    @Test
    public void testCallback() {
        final NoOpCallback cb = new NoOpCallback();

        cb.callback(null); // No exception. Nothing.
    }

    /**
     * Test method for {@link NoOpCallback#exception}.
     */
    @Test
    public void testException() {
        final NoOpCallback cb = new NoOpCallback();

        cb.exception(null); // No exception. Nothing.
    }

    /**
     * Test method for {@link NoOpCallback#isLightWeight()}.
     */
    @Test
    public void testIsLightWeight() {
        final NoOpCallback cb = new NoOpCallback();

        assertThat(cb.isLightWeight(), is(true));
    }
}
