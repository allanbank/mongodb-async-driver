/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * ServerLatencyCallback provides a special callback to measure the latency of
 * requests to a server.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyCallback extends FutureCallback<Reply> {

    /** The server to update the latency for. */
    private final ServerState myServer;

    /** The {@link System#nanoTime()} when the callback was created. */
    private final long myStartTimeNanos;

    /**
     * Creates a new ServerLatencyCallback.
     * 
     * @param server
     *            The server we are tracking the latency for.
     */
    public ServerLatencyCallback(final ServerState server) {
        super();
        myServer = server;
        myStartTimeNanos = System.nanoTime();
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
        final long end = System.nanoTime();
        final double delta = end - myStartTimeNanos;
        final double milliDelta = delta / TimeUnit.MILLISECONDS.toNanos(1);

        if (myServer != null) {
            myServer.updateAverageLatency(milliDelta);
            myServer.setTags(extractTags(result));
        }

        super.callback(result);
    }

    // Do not override the exception(Throwable) since we cannot determine the
    // latency or tags from an exception.

    /**
     * Extract any tags from the reply.
     * 
     * @param reply
     *            The reply.
     * @return The tags document, which may be <code>null</code>.
     */
    private Document extractTags(final Reply reply) {
        Document result = null;
        final List<Document> replyDocs = reply.getResults();
        if (replyDocs.size() >= 1) {
            final Document doc = replyDocs.get(0);
            final List<DocumentElement> tags = doc.queryPath(
                    DocumentElement.class, "tags");
            if (!tags.isEmpty()) {
                result = tags.get(0).asDocument();
            }
        }
        return result;
    }
}
