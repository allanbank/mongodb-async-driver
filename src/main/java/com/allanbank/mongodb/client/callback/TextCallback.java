/*
 * #%L
 * TextCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.client.SimpleMongoIteratorImpl;

/**
 * TextCallback provides conversion from a
 * {@link com.allanbank.mongodb.builder.Text text} command's result document to
 * a {@link com.allanbank.mongodb.builder.TextResult}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @deprecated Support for the {@code text} command was deprecated in the 2.6
 *             version of MongoDB. Use the {@link ConditionBuilder#text(String)
 *             $text} query operator instead. This class will not be removed
 *             until two releases after the MongoDB 2.6 release (e.g. 2.10 if
 *             the releases are 2.8 and 2.10).
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class TextCallback implements Callback<MongoIterator<Document>> {

    /**
     * The delegate callback to receive the
     * {@link com.allanbank.mongodb.builder.TextResult}s.
     * 
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This class will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    private final Callback<MongoIterator<com.allanbank.mongodb.builder.TextResult>> myDelegate;

    /**
     * Creates a new TextCallback.
     * 
     * @param delegate
     *            The delegate callback to receive the
     *            {@link com.allanbank.mongodb.builder.TextResult}s.
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This class will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    public TextCallback(
            final Callback<MongoIterator<com.allanbank.mongodb.builder.TextResult>> delegate) {
        myDelegate = delegate;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to convert each document into a
     * {@link com.allanbank.mongodb.builder.TextResult} and forward to the
     * delegate.
     * </p>
     * 
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This class will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Override
    @Deprecated
    public void callback(final MongoIterator<Document> result) {
        final List<com.allanbank.mongodb.builder.TextResult> results = new ArrayList<com.allanbank.mongodb.builder.TextResult>();

        for (final Document doc : result) {
            results.add(new com.allanbank.mongodb.builder.TextResult(doc));
        }

        myDelegate
                .callback(new SimpleMongoIteratorImpl<com.allanbank.mongodb.builder.TextResult>(
                        results));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forward to the delegate callback.
     * </p>
     * 
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This class will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Override
    @Deprecated
    public void exception(final Throwable thrown) {
        myDelegate.exception(thrown);
    }
}
