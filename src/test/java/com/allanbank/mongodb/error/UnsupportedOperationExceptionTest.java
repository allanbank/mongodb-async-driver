/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.IsMaster;

/**
 * UnsupportedOperationExceptionTest provides tests for the
 * {@link UnsupportedOperationException} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UnsupportedOperationExceptionTest {

    /**
     * Test method for
     * {@link UnsupportedOperationException#UnsupportedOperationException} .
     */
    @Test
    public void testDocumentToLargeException() {
        final Version actual = Version.parse("1.2.1");
        final Version required = Version.parse("1.2.3");
        final Message message = new IsMaster();

        final UnsupportedOperationException ex = new UnsupportedOperationException(
                "isMaster", required, actual, message);

        assertThat(
                ex.getMessage(),
                is("Attempted to send the 'isMaster' operation to a "
                        + "1.2.1 server but the operation is only supported after 1.2.3."));
        assertThat(ex.getActualVersion(), sameInstance(actual));
        assertThat(ex.getOperation(), is("isMaster"));
        assertThat(ex.getRequiredVersion(), sameInstance(required));
        assertThat(ex.getOperationsMessage(), sameInstance(message));
    }
}
