/*
 * #%L
 * ReplyException.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.error;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * Base class exception for all reply errors.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class ReplyException
        extends MongoDbException {

    /** The serialization version of the class. */
    private static final long serialVersionUID = -2597835377434607342L;

    /** The value of the "errNo" field in the reply document. */
    private final int myErrorNumber;

    /** The value of the "ok" field in the reply document. */
    private final int myOkValue;

    /** The reply. */
    private transient final Reply myReply;

    /** The original request message, if known. */
    private transient final Message myRequest;

    /**
     * Create a new ReplyException.
     *
     * @param okValue
     *            The value of the "ok" field in the reply document.
     * @param errorNumber
     *            The value of the "errNo" field in the reply document.
     * @param errorMessage
     *            The value of the 'errormsg" field in the reply document.
     * @param request
     *            The request that caused the error.
     * @param reply
     *            The reply with the error.
     */
    public ReplyException(final int okValue, final int errorNumber,
            final String errorMessage, final Message request, final Reply reply) {
        super(errorMessage);
        myOkValue = okValue;
        myErrorNumber = errorNumber;
        myRequest = request;
        myReply = reply;
    }

    /**
     * Create a new ReplyException.
     *
     * @param okValue
     *            The value of the "ok" field in the reply document.
     * @param errorNumber
     *            The value of the "errNo" field in the reply document.
     * @param errorMessage
     *            The value of the 'errmsg" field in the reply document.
     * @param reply
     *            The reply with the error.
     */
    public ReplyException(final int okValue, final int errorNumber,
            final String errorMessage, final Reply reply) {
        this(okValue, errorNumber, errorMessage, null, reply);
    }

    /**
     * Create a new ReplyException.
     *
     * @param reply
     *            The reply that raised the exception.
     */
    public ReplyException(final Reply reply) {
        super();

        myOkValue = -1;
        myErrorNumber = -1;
        myRequest = null;
        myReply = reply;
    }

    /**
     * Create a new ReplyException.
     *
     * @param reply
     *            The reply that raised the exception.
     * @param message
     *            Reason for the error.
     */
    public ReplyException(final Reply reply, final String message) {
        super(message);
        myOkValue = -1;
        myErrorNumber = -1;
        myRequest = null;
        myReply = reply;
    }

    /**
     * Create a new ReplyException.
     *
     * @param reply
     *            The reply that raised the exception.
     * @param cause
     *            If known the cause of the exception.
     */
    public ReplyException(final Reply reply, final Throwable cause) {
        super(cause);

        myOkValue = -1;
        myErrorNumber = -1;
        myRequest = null;
        myReply = reply;
    }

    /**
     * Returns the value of the "errNo" field in the reply document or -1.
     *
     * @return The value of the "errNo" field in the reply document.
     */
    public int getErrorNumber() {
        return myErrorNumber;
    }

    /**
     * Returns the value of the "ok" field in the reply document or -1.
     *
     * @return The value of the "ok" field in the reply document.
     */
    public int getOkValue() {
        return myOkValue;
    }

    /**
     * Returns the reply.
     *
     * @return The reply.
     */
    public Reply getReply() {
        return myReply;
    }

    /**
     * Returns the original request message, if known.
     *
     * @return The original request message, if known.
     */
    public Message getRequest() {
        return myRequest;
    }
}
