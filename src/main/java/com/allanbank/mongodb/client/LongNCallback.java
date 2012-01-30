/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to extract the 'n' field from the reply.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class LongNCallback extends AbstractReplyCallback<Long> {

    /**
     * Create a new NCallback.
     * 
     * @param results
     *            The callback to notify of the 'n' value.
     */
    public LongNCallback(final Callback<Long> results) {
        super(results);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creates an exception from the {@link Reply} if the 'n' field is missing.
     * </p>
     * 
     * @param reply
     *            The raw reply.
     * @return The exception created.
     */
    @Override
    protected MongoDbException asError(final Reply reply) {
        MongoDbException error = super.asError(reply);
        if (error == null) {
            final List<Document> results = reply.getResults();
            if (results.size() == 1) {
                final Document doc = results.get(0);
                final Element nElem = doc.get("n");
                if (toInt(nElem) < 0) {
                    error = new ReplyException(reply,
                            "Missing 'n' field in reply.");
                }
            }
        }
        return error;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the 'n' field in the reply document.
     * </p>
     * 
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected Long convert(final Reply reply) throws MongoDbException {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final Element nElem = doc.get("n");
            return Long.valueOf(toLong(nElem));
        }

        return Long.valueOf(-1);
    }

    /**
     * Converts a {@link DoubleElement} or {@link IntegerElement} into an
     * <tt>int</tt> value. If not a {@link DoubleElement} of
     * {@link IntegerElement} then -1 is returned.
     * 
     * @param element
     *            The element to convert.
     * @return The element's integer value or -1.
     */
    protected long toLong(final Element element) {
        if (element instanceof NumericElement) {
            return ((NumericElement) element).getLongValue();
        }

        return -1;
    }
}