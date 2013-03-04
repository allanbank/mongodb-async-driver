/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.Iterator;

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
    // Nothing to add.
}
