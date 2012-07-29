/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.Closeable;
import java.io.IOException;

import org.junit.Test;

/**
 * IOUtilsTest provides tests for the {@link IOUtils} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IOUtilsTest {
    /**
     * Test method for {@link IOUtils#close(Closeable)}.
     * 
     * @throws IOException
     *             On a failure setting up the test mocks.
     */
    @Test
    public void testClose() throws IOException {
        final Closeable mockCloseable = createMock(Closeable.class);

        mockCloseable.close();
        expectLastCall().andThrow(new IOException("This is a test."));

        replay(mockCloseable);

        IOUtils.close(mockCloseable);

        verify(mockCloseable);
    }

}
