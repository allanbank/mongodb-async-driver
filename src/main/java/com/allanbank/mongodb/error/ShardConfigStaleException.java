/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.connection.message.Reply;

/**
 * Exception raised when a a request to a shard server is based on an invalid or
 * stale configuration. Clients should never see this exception.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardConfigStaleException extends ReplyException {

    /** The serialization version for the class. */
    private static final long serialVersionUID = -3588171889388956082L;

    /**
     * Create a new ShardConfigStaleException.
     * 
     * @param reply
     *            The reply that raised the exception.
     * @param cause
     *            If known the cause of the exception.
     */
    public ShardConfigStaleException(final Reply reply, final Throwable cause) {
        super(reply, cause);
    }

}
