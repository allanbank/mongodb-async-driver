/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.client.MongoIteratorImpl;
import com.allanbank.mongodb.client.message.Reply;

/**
 * Callback to convert a {@link Reply} into a single document.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class SingleDocumentCallback extends
        AbstractReplyCallback<Document> {

    /**
     * Create a new CursorCallback.
     *
     * @param results
     *            The callback to update once the first set of results are
     *            ready.
     */
    public SingleDocumentCallback(final Callback<Document> results) {
        super(results);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to construct a {@link MongoIteratorImpl} around the reply.
     * </p>
     *
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected Document convert(final Reply reply) throws MongoDbException {
        if (!reply.getResults().isEmpty()) {
            return reply.getResults().get(0);
        }
        return null;
    }
}