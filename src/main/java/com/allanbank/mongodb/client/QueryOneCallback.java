/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Callback to convert a {@link Query} {@link Reply} into a single document.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class QueryOneCallback extends
        AbstractReplyCallback<Document> {

    /**
     * Create a new QueryCallback.
     * 
     * @param results
     *            The callback to update once the first set of results are
     *            ready.
     */
    public QueryOneCallback(final Callback<Document> results) {
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