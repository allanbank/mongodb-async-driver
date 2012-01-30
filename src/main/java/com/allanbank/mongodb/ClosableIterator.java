/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.Iterator;

/**
 * ClosableIterator provides an interface for an interator that can be closed.
 * 
 * @param <T>
 *            The type of elements being iterated over.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface ClosableIterator<T> extends Iterator<T>, Iterable<T> {

    /**
     * Close the iterator and release any resources it is holding.
     */
    public void close();
}
