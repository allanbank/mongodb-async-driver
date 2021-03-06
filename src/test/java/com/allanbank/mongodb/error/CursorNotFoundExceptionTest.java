/*
 * #%L
 * CursorNotFoundExceptionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.message.Reply;

/**
 * CursorNotFoundExceptionTest provides tests for the
 * {@link CursorNotFoundException}.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CursorNotFoundExceptionTest {

    /**
     * Test method for
     * {@link CursorNotFoundException#CursorNotFoundException(Reply, Throwable)}
     * .
     */
    @Test
    public void testCursorNotFoundException() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);
        final Throwable t = new Throwable();

        final CursorNotFoundException cnfe = new CursorNotFoundException(r, t);
        assertSame(r, cnfe.getReply());
        assertSame(t, cnfe.getCause());
    }

    /**
     * Test method for
     * {@link CursorNotFoundException#CursorNotFoundException(Reply, String)} .
     */
    @Test
    public void testCursorNotFoundExceptionWithoutCause() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);
        final String msg = "This is a test...";

        final CursorNotFoundException cnfe = new CursorNotFoundException(r, msg);
        assertSame(r, cnfe.getReply());
        assertSame(msg, cnfe.getMessage());
        assertNull(cnfe.getCause());
    }
}
