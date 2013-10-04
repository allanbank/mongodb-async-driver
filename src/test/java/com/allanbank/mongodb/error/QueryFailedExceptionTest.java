/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.message.Reply;

/**
 * QueryFailedExceptionTest provides tests for the {@link QueryFailedException}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryFailedExceptionTest {

    /**
     * Test method for
     * {@link QueryFailedException#QueryFailedException(Reply, Throwable)}.
     */
    @Test
    public void testQueryFailedException() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);
        final Throwable t = new Throwable();

        final QueryFailedException qfe = new QueryFailedException(r, t);
        assertSame(r, qfe.getReply());
        assertSame(t, qfe.getCause());
    }
}
