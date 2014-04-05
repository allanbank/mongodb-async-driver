/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * ConnectionModel provides an enumeration of the connection models that the
 * driver supports. Currently this is related to the number of threads used by
 * the socket connections to the server.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum ConnectionModel {

    /**
     * Each sender thread writes the message to the socket connection directly.
     * A single receive thread is used per connection to receive the replies.
     * While a sender writes each message to the socket the driver still has the
     * ability to batch multiple send requests in a packet but batching is
     * limited to concurrent senders. A single threaded application will not
     * batch requests.
     * <p>
     * This is the default {@code ConnectionModel}.
     * </p>
     * <p>
     * In a multi-threaded application this {@code ConnectionModel} should
     * perform as well as if not better than the {@link #SENDER_RECEIVER_THREAD}
     * model. This is achieved by completely avoiding cross thread passing of
     * the message on a send.
     * </p>
     */
    RECEIVER_THREAD,

    /**
     * Each socket uses a pair of threads: sender and receiver.
     * <p>
     * This was the default {@code ConnectionModel} versions prior to 1.3.0.
     * </p>
     * <p>
     * This {@code ConnectionModel} is most useful for connections where a
     * single {@code write()} to the socket implementation is slow and the
     * application only uses a single write thread.
     * </p>
     *
     * @since 1.0.0
     */
    SENDER_RECEIVER_THREAD;
}
