/*
 * #%L
 * ReplyDocumentCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;

/**
 * Callback to expect and extract a single document from the reply and then
 * extract its contained document.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyDocumentCallback
        extends AbstractReplyCallback<Document> {

    /** The default name for the reply's document to return. */
    public static final String DEFAULT_NAME = "value";

    /** The name of the document in the reply document. */
    private final String myName;

    /**
     * Create a new ReplyDocumentCallback.
     *
     * @param results
     *            The callback to notify of the reply document.
     */
    public ReplyDocumentCallback(final Callback<Document> results) {
        this(DEFAULT_NAME, results);
    }

    /**
     * Create a new ReplyDocumentCallback.
     *
     * @param name
     *            The name of the document in the reply document to extract.
     * @param results
     *            The callback to notify of the reply document.
     */
    public ReplyDocumentCallback(final String name,
            final Callback<Document> results) {
        super(results);

        myName = name;
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
            else if (reply.getResults().get(0).get(myName) == null) {
                error = new ReplyException(reply, "No '" + myName
                        + "' document in the reply.");
            }
        }
        return error;
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
            final DocumentElement element = results.get(0).get(
                    DocumentElement.class, myName);
            if (element == null) {
                return null;
            }
            return element.getDocument();
        }

        return null;
    }
}