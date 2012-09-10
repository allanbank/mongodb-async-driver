/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
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
 * <p>
 * In the case of an exception the seconds behind is set to
 * {@link Integer#MAX_VALUE}. The value is configurable as a long so in theory a
 * user can ignore this case using a large
 * {@link com.allanbank.mongodb.MongoDbConfiguration#setMaxSecondaryLag(long)}.
 * </p>
 * <p>
 * Lastly, the state of the server is also checked and the seconds behind is set
 * to {@link Double#MAX_VALUE} if not in the primary ({@value #PRIMARY_STATE})
 * or secondary ({@value #SECONDARY_STATE}).
 * </p>
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SecondsBehindCallback extends FutureCallback<Reply> {

    /** The numeric element type. */
    public static final Class<NumericElement> NUMERIC_TYPE = NumericElement.class;

    /** The timestamp element type. */
    public static final Class<TimestampElement> TIMESTAMP_TYPE = TimestampElement.class;

    /** The document element type. */
    public static final Class<DocumentElement> DOCUMENT_TYPE = DocumentElement.class;

    /** The value for a primary server's state. */
    public static final int PRIMARY_STATE = 1;

    /** The value for a secondary (actively replicating) server's state. */
    public static final int SECONDARY_STATE = 2;

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
            myServer.setSecondsBehind(Integer.MAX_VALUE);
        }
    }

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

            NumericElement state = doc.get(NUMERIC_TYPE, "myState");
            if ((state != null)
                    && ((state.getIntValue() == PRIMARY_STATE) || (state
                            .getIntValue() == SECONDARY_STATE))) {

                TimestampElement serverTimestamp = null;
                final StringElement expectedName = new StringElement("name",
                        myServer.getName());
                for (final DocumentElement member : doc.find(DOCUMENT_TYPE,
                        "members", ".*")) {
                    if (expectedName.equals(member.get("name"))
                            && (member.get(TIMESTAMP_TYPE, "optimeDate") != null)) {

                        serverTimestamp = member.get(TIMESTAMP_TYPE,
                                "optimeDate");
                    }
                }

                if (serverTimestamp != null) {
                    TimestampElement latestTimestamp = serverTimestamp;
                    for (final TimestampElement time : doc.find(TIMESTAMP_TYPE,
                            "members", ".*", "optimeDate")) {
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
            else {
                // "myState" != 1 and "myState" != 2
                myServer.setSecondsBehind(Double.MAX_VALUE);
            }

        }
    }
}
