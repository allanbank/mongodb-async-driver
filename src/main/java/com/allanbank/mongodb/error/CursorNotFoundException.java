/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.client.message.Reply;

/**
 * Exception raised when a get_more request is issued for an unknown cursor.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CursorNotFoundException extends ReplyException {

    /** The serialization version for the class. */
    private static final long serialVersionUID = -3588171889388956082L;

    /**
     * Create a new CursorNotFoundException.
     * 
     * @param reply
     *            The reply that raised the exception.
     * @param message
     *            Reason for the error.
     */
    public CursorNotFoundException(final Reply reply, final String message) {
        super(reply, message);
    }

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
