/*
 * #%L
 * IteratorToListCallbackAdapterTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * IteratorToListCallbackAdapterTest provides tests for the
 * {@link IteratorToListCallbackAdapter} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IteratorToListCallbackAdapterTest {

    /**
     * Test method for
     * {@link IteratorToListCallbackAdapter#callback(MongoIterator)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallback() {

        final Document doc = BuilderFactory.start().build();

        final MongoIterator<Document> mockIterator = createMock(MongoIterator.class);
        final Callback<List<Document>> mockDelegate = createMock(Callback.class);

        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andReturn(false);

        mockIterator.close();
        expectLastCall();

        mockDelegate.callback(Arrays.asList(doc, doc, doc));
        expectLastCall();

        replay(mockIterator, mockDelegate);

        final IteratorToListCallbackAdapter adapter = new IteratorToListCallbackAdapter(
                mockDelegate);

        adapter.callback(mockIterator);

        verify(mockIterator, mockDelegate);
    }

    /**
     * Test method for
     * {@link IteratorToListCallbackAdapter#callback(MongoIterator)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallbackWithException() {
        final RuntimeException toThrow = new RuntimeException();
        final Document doc = BuilderFactory.start().build();

        final MongoIterator<Document> mockIterator = createMock(MongoIterator.class);
        final Callback<List<Document>> mockDelegate = createMock(Callback.class);

        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andReturn(true);
        expect(mockIterator.next()).andReturn(doc);
        expect(mockIterator.hasNext()).andThrow(toThrow);

        mockIterator.close();
        expectLastCall();

        mockDelegate.callback(Arrays.asList(doc, doc, doc));
        expectLastCall();

        replay(mockIterator, mockDelegate);

        final IteratorToListCallbackAdapter adapter = new IteratorToListCallbackAdapter(
                mockDelegate);
        try {
            adapter.callback(mockIterator);
        }
        catch (final RuntimeException error) {
            assertThat(error, sameInstance(toThrow));
        }

        verify(mockIterator, mockDelegate);
    }

    /**
     * Test method for
     * {@link IteratorToListCallbackAdapter#exception(Throwable)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testException() {
        final Throwable thrown = new Error();

        final Callback<List<Document>> mockDelegate = createMock(Callback.class);

        mockDelegate.exception(thrown);
        expectLastCall();

        replay(mockDelegate);

        final IteratorToListCallbackAdapter adapter = new IteratorToListCallbackAdapter(
                mockDelegate);

        adapter.exception(thrown);

        verify(mockDelegate);
    }

}
