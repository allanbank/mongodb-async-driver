/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.connection.message.Reply;

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
