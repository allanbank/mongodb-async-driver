/*
 * #%L
 * ServerUpdateCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.state;

import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.message.Reply;

/**
 * ServerUpdateCallback provides a special callback update the server with the
 * first document in the reply. This is useful with {@code ismaster} or
 * {@code replSetGetStatus} commands.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerUpdateCallback extends FutureReplyCallback {

    /** The server to update the seconds behind for. */
    private final Server myServer;

    /**
     * Creates a new ServerUpdateCallback.
     * 
     * @param server
     *            The server we are tracking the latency for.
     */
    public ServerUpdateCallback(final Server server) {
        super();
        myServer = server;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to update the server's latency based on the round trip reply
     * time.
     * </p>
     */
    @Override
    public void callback(final Reply result) {
        if (myServer != null) {
            update(result);
        }

        super.callback(result);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to mark the server as {@link Integer#MAX_VALUE} seconds behind
     * the primary.
     * </p>
     */
    @Override
    public void exception(final Throwable error) {
        if (myServer != null) {
            myServer.requestFailed();
        }

        super.exception(error);
    }

    /**
     * Updates the server with the first document from the reply.
     * 
     * @param reply
     *            The reply.
     */
    private void update(final Reply reply) {
        if (reply != null) {
            final List<Document> replyDocs = reply.getResults();
            if (replyDocs.size() >= 1) {
                final Document doc = replyDocs.get(0);

                myServer.update(doc);
            }
        }
    }
}
