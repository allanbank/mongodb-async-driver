/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.error;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * Exception raised when a request to the server exceeds the maximum allowed
 * time.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MaximumTimeLimitExceededException extends ReplyException {

    /** The serialization version for the class. */
    private static final long serialVersionUID = 2947219604194689861L;

    /**
     * Create a new MaximumTimeLimitExceededException.
     * 
     * @param okValue
     *            The value of the "ok" field in the reply document.
     * @param errorNumber
     *            The value of the "errNo" field in the reply document.
     * @param errorMessage
     *            The value of the 'errmsg" field in the reply document.
     * @param message
     *            The message that triggered the reply.
     * @param reply
     *            The reply with the error.
     */
    public MaximumTimeLimitExceededException(final int okValue,
            final int errorNumber, final String errorMessage,
            final Message message, final Reply reply) {
        super(okValue, errorNumber, errorMessage, reply);
    }

}
