/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * Callback to extract the map/reduce results from the reply.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MapReduceReplyCallback extends
        AbstractReplyCallback<List<Document>> {

    /**
     * Create a new MapReduceReplyCallback.
     * 
     * @param forwardCallback
     *            The callback to forward the result documents to.
     */
    public MapReduceReplyCallback(final Callback<List<Document>> forwardCallback) {
        super(forwardCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to extract the 'results' elements from the reply.
     * </p>
     * 
     * @see com.allanbank.mongodb.client.AbstractReplyCallback#convert(com.allanbank.mongodb.connection.message.Reply)
     */
    @Override
    protected List<Document> convert(final Reply reply) throws MongoDbException {
        List<Document> results = Collections.emptyList();

        final List<Document> replyDocs = reply.getResults();
        if (replyDocs.size() == 1) {
            final Document doc = replyDocs.get(0);
            final List<DocumentElement> resultsElems = doc.queryPath(
                    DocumentElement.class, "results", ".*");
            if (!resultsElems.isEmpty()) {
                results = new ArrayList<Document>();
                for (final DocumentElement resultElem : resultsElems) {
                    results.add(resultElem.getDocument());
                }
            }
        }

        return results;
    }

}
