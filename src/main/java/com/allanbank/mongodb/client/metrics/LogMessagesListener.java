/*
 * #%L
 * AbstractMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * LogMessagesListener provides a {@link MongoMessageListener} that logs each
 * message that is sent or received to the
 * {@value MongoClientConfiguration#MESSAGE_LOGGER_NAME} logger at the debug
 * level.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LogMessagesListener implements MongoMessageListener {

    /**
     * The logger for the {@value MongoClientConfiguration#MESSAGE_LOGGER_NAME}.
     */
    protected static final Log LOG = LogFactory
            .getLog(MongoClientConfiguration.MESSAGE_LOGGER_NAME);

    /**
     * Creates a new LogMessagesListener.
     */
    public LogMessagesListener() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to log the reply received.
     * </p>
     */
    @Override
    public void receive(final String serverName, final long messageId,
            final Message sent, final Reply reply, final long latencyNanos) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}:{} ({} ns) <-- {}", serverName,
                    Long.valueOf(messageId), Long.valueOf(latencyNanos), reply);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to log the message sent.
     * </p>
     */
    @Override
    public void sent(final String serverName, final long messageId,
            final Message sent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}:{} --> {}", serverName, Long.valueOf(messageId), sent);
        }
    }
}
