/*
 * #%L
 * ClosableIterator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * ClosableIterator provides an interface for an iterator that can be closed.
 * <p>
 * In addition the batch size for the next request for documents from the cursor
 * can be set.
 * </p>
 *
 * @param <T>
 *            The type of elements being iterated over.
 *
 * @deprecated Use the {@link MongoIterator} interface instead. This interface
 *             will be removed after the 1.3.0 release.
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
@NotThreadSafe
public interface ClosableIterator<T>
        extends Iterator<T>, Iterable<T> {

    /**
     * Close the iterator and release any resources it is holding.
     */
    public void close();

    /**
     * Returns the size for batches of documents that are requested.
     *
     * @return The size of the batches of documents that are requested.
     */
    public int getBatchSize();

    /**
     * Sets the size for future batch sizes.
     *
     * @param batchSize
     *            The size to request for future batch sizes.
     */
    public void setBatchSize(int batchSize);
}
