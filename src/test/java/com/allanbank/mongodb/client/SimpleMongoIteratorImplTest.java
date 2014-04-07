/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Iterator;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.impl.EmptyDocument;

/**
 * SimpleMongoIteratorImplTest provides tests for the
 * {@link SimpleMongoIteratorImpl}.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleMongoIteratorImplTest {

    /**
     * Test method for {@link SimpleMongoIteratorImpl#asDocument()}.
     */
    @Test
    public void testAsDocument() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        assertThat(testInstance.asDocument(), nullValue());

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#close()}.
     */
    @Test
    public void testClose() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // Does nothing.
        testInstance.close();

        assertThat(testInstance.hasNext(), is(true));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#getBatchSize()}.
     */
    @Test
    public void testGetBatchSize() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.getBatchSize(), is(-1));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#hasNext()}.
     */
    @Test
    public void testHasNext() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.hasNext(), is(true));
        assertThat(testInstance.next(),
                sameInstance((Document) EmptyDocument.INSTANCE));
        assertThat(testInstance.hasNext(), is(false));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#iterator()}.
     */
    @Test
    public void testIterator() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.iterator(),
                sameInstance((Iterator<Document>) testInstance));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#remove()}.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        testInstance.remove();

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#setBatchSize(int)}.
     */
    @Test
    public void testSetBatchSize() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        testInstance.setBatchSize(10);
        assertThat(testInstance.getBatchSize(), is(-1));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#stop()}.
     */
    @Test
    public void testStop() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        testInstance.stop();

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#toArray()}.
     */
    @Test
    public void testToArray() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.toArray(),
                arrayContaining((Object) EmptyDocument.INSTANCE));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#toArray}.
     */
    @Test
    public void testToArraySArray() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.toArray(new Document[0]),
                arrayContaining((Document) EmptyDocument.INSTANCE));

        testInstance.close();
    }

    /**
     * Test method for {@link SimpleMongoIteratorImpl#toList()}.
     */
    @Test
    public void testToList() {
        final SimpleMongoIteratorImpl<Document> testInstance = new SimpleMongoIteratorImpl<Document>(
                Collections.singletonList((Document) EmptyDocument.INSTANCE));

        // No need for batches. Already in memory.
        assertThat(testInstance.toList(),
                contains((Document) EmptyDocument.INSTANCE));

        testInstance.close();
    }

}
