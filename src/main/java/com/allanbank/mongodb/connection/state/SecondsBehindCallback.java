/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * SecondsBehindCallback provides a special callback to measure the number of
 * seconds a replica is behind the primary.
 * <p>
 * To account for idle servers we use the optime for each server and assign a
 * value of zero to the "latest" optime and then subtract the remaining servers
 * from that optime.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SecondsBehindCallback extends FutureCallback<Reply> {

    /** The server to update the seconds behind for. */
    private final ServerState myServer;

    /**
     * Creates a new SecondsBehindCallback.
     * 
     * @param server
     *            The server we are tracking the latency for.
     */
    public SecondsBehindCallback(final ServerState server) {
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
            updateSecondsBehind(result);
        }

        super.callback(result);
    }

    // Do not override the exception(Throwable) since we cannot determine the
    // seconds behind from an exception.

    /**
     * Extract the number of seconds this Server is behind the primary by
     * comparing its latest optime with that of the absolute latest optime.
     * 
     * @param reply
     *            The reply.
     */
    private void updateSecondsBehind(final Reply reply) {
        final List<Document> replyDocs = reply.getResults();
        if (replyDocs.size() >= 1) {
            final Document doc = replyDocs.get(0);

            TimestampElement serverTimestamp = null;
            final StringElement expectedName = new StringElement("name",
                    myServer.getName());
            for (final DocumentElement member : doc.queryPath(
                    DocumentElement.class, "members", ".*")) {
                if (expectedName.equals(member.get("name"))
                        && (member.get("optimeDate") instanceof TimestampElement)) {
                    
                    serverTimestamp = (TimestampElement) member
                            .get("optimeDate");
                }
            }

            if (serverTimestamp != null) {
                TimestampElement latestTimestamp = serverTimestamp;
                for (final TimestampElement time : doc.queryPath(
                        TimestampElement.class, "members", ".*", "optimeDate")) {
                    if (latestTimestamp.getTime() < time.getTime()) {
                        latestTimestamp = time;
                    }
                }

                final double msBehind = latestTimestamp.getTime()
                        - serverTimestamp.getTime();
                myServer.setSecondsBehind(msBehind
                        / TimeUnit.SECONDS.toMillis(1));
            }
        }
    }
}
