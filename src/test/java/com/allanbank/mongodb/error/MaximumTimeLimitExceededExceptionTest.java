/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * MaximumTimeLimitExceededExceptionTest provides tests for the
 * {@link MaximumTimeLimitExceededException}.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MaximumTimeLimitExceededExceptionTest {

    /**
     * Test method for
     * {@link MaximumTimeLimitExceededException#MaximumTimeLimitExceededException(int, int, String, Message, Reply)}
     * .
     */
    @Test
    public void testMaximumTimeLimitExceededException() {
        final List<Document> docs = Collections.emptyList();

        final Reply r = new Reply(1, 1, 1, docs, false, false, false, false);

        final MaximumTimeLimitExceededException re = new MaximumTimeLimitExceededException(
                1234, 12345, "foo", null, r);
        assertSame(r, re.getReply());
        assertEquals("foo", re.getMessage());
        assertNull(re.getCause());
        assertEquals(12345, re.getErrorNumber());
        assertEquals(1234, re.getOkValue());
        assertNull(re.getRequest());
    }
}
