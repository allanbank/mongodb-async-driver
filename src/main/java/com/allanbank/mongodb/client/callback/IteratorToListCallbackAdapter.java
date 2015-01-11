/*
 * #%L
 * IteratorToListCallbackAdapter.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
public class IteratorToListCallbackAdapter
        implements Callback<MongoIterator<Document>> {

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
