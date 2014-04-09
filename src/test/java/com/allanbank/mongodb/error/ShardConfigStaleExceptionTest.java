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
 * ShardConfigStaleExceptionTest provides tests for the
 * {@link ShardConfigStaleException}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardConfigStaleExceptionTest {

    /**
     * Test method for
     * {@link ShardConfigStaleException#ShardConfigStaleException(Reply, Throwable)}
     * .
     */
    @Test
    public void testShardConfigStaleException() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);
        final Throwable t = new Throwable();

        final ShardConfigStaleException scse = new ShardConfigStaleException(r,
                t);
        assertSame(r, scse.getReply());
        assertSame(t, scse.getCause());
    }

}
