/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.util.IOUtils;

/**
 * IteratorToListCallbackAdapter provides the ability to translate a
 * MongoIterator callback into a list callback.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IteratorToListCallbackAdapter implements
Callback<MongoIterator<Document>> {

    /** The list callback to invoke once all of the documents are collected. */
    private final Callback<List<Document>> myDelegate;

    /**
     * Creates a new IteratorToListCallbackAdapter.
     *
     * @param delegate
     *            The list callback to invoke once all of the documents are
     *            collected.
     */
    public IteratorToListCallbackAdapter(final Callback<List<Document>> delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to collect all of the results and then call the delegate.
     * </p>
     */
    @Override
    public void callback(final MongoIterator<Document> result) {
        final List<Document> docs = new ArrayList<Document>();
        try {
            while (result.hasNext()) {
                docs.add(result.next());
            }
        }
        finally {
            IOUtils.close(result);
            myDelegate.callback(docs);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the delegate.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myDelegate.exception(thrown);
    }
}
