/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.message.Reply;

/**
 * ValidatingCallback to expect and extract a single document from the reply.
 * The document is checked for errors but then ignored.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ValidatingReplyCallback extends AbstractReplyCallback<Reply> {

    /**
     * Create a new ValidatingReplyCallback.
     * 
     * @param delegate
     *            The delegate that we can wait on for the reply.
     */
    public ValidatingReplyCallback(final FutureReplyCallback delegate) {
        super(delegate);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true as there is no additional processing.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the original reply.
     * </p>
     */
    @Override
    protected Reply convert(final Reply reply) throws MongoDbException {
        return reply;
    }
}