/*
 * #%L
 * LogMessagesListenerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * LogMessagesListenerTest provides tests for the {@link LogMessagesListener}
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LogMessagesListenerTest {

    /** The test log handler to cature any logged messages. */
    private TestHandler myHandler;

    /** The listener for messages under test. */
    private LogMessagesListener myListener;

    /** The logger to receive any logged messages. */
    private Logger myLogger;

    /**
     * Initializes the logger and listener.
     */
    @Before
    public void setUp() {
        myHandler = new TestHandler();

        myLogger = Logger
                .getLogger(MongoClientConfiguration.MESSAGE_LOGGER_NAME);
        myLogger.addHandler(myHandler);
        myListener = new LogMessagesListener();
        LogMessagesListener.LOG.reset();
    }

    /**
     * Cleans up from the test.
     */
    @After
    public void tearDown() {
        if (myLogger != null) {
            myLogger.removeHandler(myHandler);
        }
        myLogger = null;
        myHandler = null;
        myListener = null;
    }

    /**
     * Test method for
     * {@link LogMessagesListener#receive(String, long, Message, Reply, long)}.
     */
    @Test
    public void testReceive() {
        // Should not log.
        myLogger.setLevel(Level.INFO);
        myListener.receive(null, 0L, null, null, 0L);

        assertThat(myHandler.getRecord(), nullValue());
    }

    /**
     * Test method for
     * {@link LogMessagesListener#receive(String, long, Message, Reply, long)}.
     */
    @Test
    public void testReceiveLogs() {
        // Should log.
        myLogger.setLevel(Level.FINE);
        myListener.receive(null, 0L, null, null, 0L);

        assertThat(myHandler.getRecord(), not(nullValue()));

    }

    /**
     * Test method for {@link LogMessagesListener#sent(String, long, Message)} .
     */
    @Test
    public void testSent() {
        // Should not log.
        myLogger.setLevel(Level.INFO);
        myListener.sent(null, 0L, null);

        assertThat(myHandler.getRecord(), nullValue());
    }

    /**
     * Test method for {@link LogMessagesListener#sent(String, long, Message)} .
     */
    @Test
    public void testSentLogs() {
        // Should log.
        myLogger.setLevel(Level.FINE);
        myListener.sent(null, 0L, null);

        assertThat(myHandler.getRecord(), not(nullValue()));
    }

    /**
     * TestHandler provides a test log handler.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class TestHandler
            extends Handler {

        /** The last record published. */
        private LogRecord myRecord = null;

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws SecurityException {
            myRecord = null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void flush() {
            // Nothing

        }

        /**
         * Returns the last record.
         *
         * @return The last record.
         */
        public LogRecord getRecord() {
            return myRecord;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void publish(final LogRecord record) {
            myRecord = record;
        }
    }

}
