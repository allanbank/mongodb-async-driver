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
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to expect and extract a single document from the reply and then
 * extract a contained {@link NumericElement} and coerce it to a long value.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyLongCallback extends AbstractReplyCallback<Long> {

    /** The default name for the long value to return. */
    public static final String DEFAULT_NAME = "n";

    /**
     * The name of the {@link NumericElement value} expected in the reply
     * document.
     */
    private final String myName;

    /**
     * Create a new ReplyLongCallback.
     *
     * @param results
     *            The callback to notify of the value.
     */
    public ReplyLongCallback(final Callback<Long> results) {
        this(DEFAULT_NAME, results);
    }

    /**
     * Create a new ReplyLongCallback.
     *
     * @param name
     *            The name of the {@link NumericElement numeric} value.
     * @param results
     *            The callback to notify of the value.
     */
    public ReplyLongCallback(final String name, final Callback<Long> results) {
        super(results);

        myName = name;
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
                final Element nElem = doc.get(myName);
                if (!(nElem instanceof NumericElement)) {
                    error = new ReplyException(reply, "Missing '" + myName
                            + "' field in reply.");
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
            final Element nElem = doc.get(myName);
            return Long.valueOf(toLong(nElem));
        }

        return Long.valueOf(-1);
    }

    /**
     * Converts a {@link NumericElement} into a <tt>long</tt> value. If not a
     * {@link NumericElement} then -1 is returned.
     *
     * @param element
     *            The element to convert.
     * @return The element's long value or -1.
     */
    protected long toLong(final Element element) {
        if (element instanceof NumericElement) {
            return ((NumericElement) element).getLongValue();
        }

        return -1;
    }
}