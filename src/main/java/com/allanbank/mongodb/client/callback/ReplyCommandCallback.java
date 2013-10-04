/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to expect and extract a single document from the reply.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyCommandCallback extends AbstractReplyCallback<Document> {

    /**
     * Create a new ReplyDocumentCallback.
     * 
     * @param results
     *            The callback to notify of the reply document.
     */
    public ReplyCommandCallback(final Callback<Document> results) {
        super(results);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creates an exception if the {@link Reply} has less than or more than a
     * single reply document.
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
            if (results.size() != 1) {
                error = new ReplyException(reply,
                        "Should only be a single document in the reply.");
            }
        }
        return error;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to not throw an exception on a zero 'ok' value.
     * </p>
     * 
     * @param reply
     *            The raw reply.
     * @param knownError
     *            If true then the reply is assumed to be an error reply.
     * @return The exception created.
     */
    @Override
    protected MongoDbException asError(final Reply reply,
            final boolean knownError) {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final Element okElem = doc.get("ok");
            final Element errorNumberElem = doc.get("code");
            Element errorMessageElem = null;
            for (int i = 0; (errorMessageElem == null)
                    && (i < ERROR_MESSAGE_FIELDS.size()); ++i) {
                errorMessageElem = doc.get(ERROR_MESSAGE_FIELDS.get(i));
            }

            if ((okElem == null) && knownError) {
                return asError(reply, -1, toInt(errorNumberElem),
                        asString(errorMessageElem));

            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the reply document.
     * </p>
     * 
     * @see AbstractReplyCallback#convert(Reply)
     */
    @Override
    protected Document convert(final Reply reply) throws MongoDbException {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            return results.get(0);
        }

        return null;
    }
}