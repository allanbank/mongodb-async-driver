/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Exception raised when a get_more request is issued for an unknown cursor.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CursorNotFoundException extends ReplyException {

    /** The serialization version for the class. */
    private static final long serialVersionUID = -3588171889388956082L;

    /**
     * Create a new CursorNotFoundException.
     * 
     * @param reply
     *            The reply that raised the exception.
     * @param cause
     *            If known the cause of the exception.
     */
    public CursorNotFoundException(final Reply reply, final Throwable cause) {
        super(reply, cause);
    }

}
