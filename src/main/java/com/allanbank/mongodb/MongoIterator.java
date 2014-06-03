/*
 * #%L
 * MongoIterator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.List;

/**
 * MongoIterator provides an interface for an iterator that can be closed.
 * <p>
 * In addition the batch size for the next request for documents from the cursor
 * can be set.
 * </p>
 * 
 * @param <T>
 *            The type of elements being iterated over.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings({ "deprecation", "unused" })
public interface MongoIterator<T> extends Iterator<T>, Iterable<T>,
        MongoCursorControl, ClosableIterator<T> {
    /**
     * Consumes all of the elements in the iterator and returns them in a single
     * array.
     * <p>
     * WARNING: This method loads all of the iterator results into memory and
     * may cause an {@link OutOfMemoryError}.
     * </p>
     * 
     * @return The remaining elements in the iterator.
     */
    Object[] toArray();

    /**
     * Consumes all of the elements in the iterator and returns them in a single
     * array.
     * <p>
     * WARNING: This method loads all of the iterator results into memory and
     * may cause an {@link OutOfMemoryError}.
     * </p>
     * 
     * @param <S>
     *            The type of elements in the array.
     * @param to
     *            The array to copy into. If not the right size a new array will
     *            be allocated of the right size.
     * 
     * @return The remaining elements in the iterator.
     */
    <S> S[] toArray(S[] to);

    /**
     * Consumes all of the elements in the iterator and returns them in a single
     * mutable list.
     * <p>
     * WARNING: This method loads all of the iterator results into memory and
     * may cause an {@link OutOfMemoryError}.
     * </p>
     * 
     * @return The remaining elements in the iterator.
     */
    List<T> toList();
}
