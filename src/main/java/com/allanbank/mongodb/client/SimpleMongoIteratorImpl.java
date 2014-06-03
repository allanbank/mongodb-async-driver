/*
 * #%L
 * SimpleMongoIteratorImpl.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;

/**
 * Iterator over the results of fixed collection.
 * 
 * @param <T>
 *            The type being iterated.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleMongoIteratorImpl<T> implements MongoIterator<T> {

    /** The wrapped iterator. */
    private final Iterator<T> myIterator;

    /**
     * Create a new SimpleMongoIteratorImpl.
     * 
     * @param wrapped
     *            The collection to turn into a {@link MongoIterator}.
     */
    public SimpleMongoIteratorImpl(final Collection<T> wrapped) {
        myIterator = wrapped.iterator();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the null as this cursor cannot be restarted.
     * </p>
     */
    @Override
    public Document asDocument() {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void close() {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return -1.
     * </p>
     */
    @Override
    public int getBatchSize() {
        return -1;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true if there are more documents.
     * </p>
     */
    @Override
    public boolean hasNext() {
        return myIterator.hasNext();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return this iterator.
     * </p>
     */
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the next document from the query.
     * </p>
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public T next() {
        return myIterator.next();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to throw an {@link UnsupportedOperationException}.
     * </p>
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Cannot remove a document via a MongoDB iterator.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing as this iterator only supports single batch
     * results.
     * </p>
     */
    @Override
    public void setBatchSize(final int batchSize) {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing as this iterator only supports single batch
     * results.
     * </p>
     */
    @Override
    public void stop() {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the remaining elements as a array.
     * </p>
     */
    @Override
    public Object[] toArray() {
        final List<T> remaining = toList();

        return remaining.toArray();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the remaining elements as a array.
     * </p>
     */
    @Override
    public <S> S[] toArray(final S[] to) {
        final List<T> remaining = toList();

        return remaining.toArray(to);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the remaining elements as a list.
     * </p>
     */
    @Override
    public List<T> toList() {
        final List<T> remaining = new ArrayList<T>();

        while (hasNext()) {
            remaining.add(next());
        }

        return remaining;
    }
}
