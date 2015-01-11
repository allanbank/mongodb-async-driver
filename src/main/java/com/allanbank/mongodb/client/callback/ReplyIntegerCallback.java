/*
 * #%L
 * ReplyIntegerCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * extract a contained {@link NumericElement} and coerce it to a integer value.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyIntegerCallback
        extends AbstractReplyCallback<Integer> {

    /** The default name for the long value to return. */
    public static final String DEFAULT_NAME = "n";

    /**
     * The name of the {@link NumericElement value} expected in the reply
     * document.
     */
    private final String myName;

    /**
     * Create a new ReplyIntegerCallback.
     *
     * @param results
     *            The callback to notify of the 'n' value.
     */
    public ReplyIntegerCallback(final Callback<Integer> results) {
        this(DEFAULT_NAME, results);
    }

    /**
     * Create a new ReplyIntegerCallback.
     *
     * @param name
     *            The name of the {@link NumericElement numeric} value.
     * @param results
     *            The callback to notify of the 'n' value.
     */
    public ReplyIntegerCallback(final String name,
            final Callback<Integer> results) {
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
                if (toInt(nElem) < 0) {
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
    protected Integer convert(final Reply reply) throws MongoDbException {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final Element nElem = doc.get(myName);
            return Integer.valueOf(toInt(nElem));
        }

        return Integer.valueOf(-1);
    }
}