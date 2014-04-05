/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.AbstractMongoOperations;
import com.allanbank.mongodb.error.BatchedWriteException;

/**
 * BatchedNativeWriteCallbackTest provides tests for the
 * {@link BatchedNativeWriteCallback} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedNativeWriteCallbackTest {

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNative() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc)
                .durability(Durability.fsyncDurable(100)).build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        final Capture<Callback<Integer>> capture1 = new Capture<Callback<Integer>>();
        final Capture<Callback<Long>> capture2 = new Capture<Callback<Long>>();
        final Capture<Callback<Long>> capture3 = new Capture<Callback<Long>>();

        // Send the requests.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(capture(capture1), eq(true),
                eq(Durability.fsyncDurable(100)), eq(doc));
        expectLastCall();
        mockCollection.updateAsync(capture(capture2), eq(doc), eq(doc),
                eq(false), eq(false), eq(Durability.fsyncDurable(100)));
        expectLastCall();
        mockCollection.deleteAsync(capture(capture3), eq(doc), eq(false),
                eq(Durability.fsyncDurable(100)));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the results.
        reset(mockResults, mockCollection);
        mockResults.callback(Long.valueOf(3));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture1.getValue().callback(1);
        capture2.getValue().callback(1L);
        capture3.getValue().callback(1L);
        verify(mockResults, mockCollection);
    }

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNativeAllError() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        final Capture<Callback<Integer>> capture1 = new Capture<Callback<Integer>>();
        final Capture<Callback<Long>> capture2 = new Capture<Callback<Long>>();
        final Capture<Callback<Long>> capture3 = new Capture<Callback<Long>>();

        // Send the requests.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(capture(capture1), eq(true),
                eq(Durability.ACK), eq(doc));
        expectLastCall();
        mockCollection.updateAsync(capture(capture2), eq(doc), eq(doc),
                eq(false), eq(false), eq(Durability.ACK));
        expectLastCall();
        mockCollection.deleteAsync(capture(capture3), eq(doc), eq(false),
                eq(Durability.ACK));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        final Throwable error = new Throwable("Injected!");
        final List<WriteOperation> skipped = Collections.emptyList();
        reset(mockResults, mockCollection);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture1.getValue().exception(error);
        capture2.getValue().exception(error);
        capture3.getValue().exception(error);
        verify(mockResults, mockCollection);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(3));
        assertThat(batchError.getN(), is(0L));
        assertThat(batchError.getSkipped(), is(skipped));
        assertThat(batchError.getWrite(), sameInstance(write));
    }

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNativeError() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        final Capture<Callback<Integer>> capture1 = new Capture<Callback<Integer>>();
        final Capture<Callback<Long>> capture2 = new Capture<Callback<Long>>();
        final Capture<Callback<Long>> capture3 = new Capture<Callback<Long>>();

        // Send the requests.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(capture(capture1), eq(true),
                eq(Durability.ACK), eq(doc));
        expectLastCall();
        mockCollection.updateAsync(capture(capture2), eq(doc), eq(doc),
                eq(false), eq(false), eq(Durability.ACK));
        expectLastCall();
        mockCollection.deleteAsync(capture(capture3), eq(doc), eq(false),
                eq(Durability.ACK));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        final Throwable error = new Throwable("Injected!");
        final List<WriteOperation> skipped = Collections.emptyList();
        reset(mockResults, mockCollection);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture1.getValue().callback(1);
        capture2.getValue().callback(1L);
        capture3.getValue().exception(error);
        verify(mockResults, mockCollection);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors(),
                is(Collections.singletonMap(operations.get(2), error)));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped(), is(skipped));
        assertThat(batchError.getWrite(), sameInstance(write));
    }

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNativeFirstErrorAndStop() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc)
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        final Capture<Callback<Integer>> capture1 = new Capture<Callback<Integer>>();

        // Send the requests.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(capture(capture1), eq(true),
                eq(Durability.ACK), eq(doc));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        final Throwable error = new Throwable("Injected!");
        reset(mockResults, mockCollection);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture1.getValue().exception(error);
        verify(mockResults, mockCollection);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors(),
                is(Collections.singletonMap(operations.get(0), error)));
        assertThat(batchError.getN(), is(0L));
        assertThat(batchError.getSkipped(), is(operations.subList(1, 3)));
        assertThat(batchError.getWrite(), sameInstance(write));
    }

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNativeSecondErrorAndStop() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc)
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        final Capture<Callback<Integer>> capture1 = new Capture<Callback<Integer>>();
        final Capture<Callback<Long>> capture2 = new Capture<Callback<Long>>();

        // Send the requests.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(capture(capture1), eq(true),
                eq(Durability.ACK), eq(doc));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        final Throwable error = new Throwable("Injected!");
        reset(mockResults, mockCollection);
        mockCollection.updateAsync(capture(capture2), eq(doc), eq(doc),
                eq(false), eq(false), eq(Durability.ACK));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture1.getValue().callback(1);
        verify(mockResults, mockCollection);

        reset(mockResults, mockCollection);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockCollection);
        capture2.getValue().exception(error);
        verify(mockResults, mockCollection);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors(),
                is(Collections.singletonMap(operations.get(1), error)));
        assertThat(batchError.getN(), is(1L));
        assertThat(batchError.getSkipped(), is(operations.subList(2, 3)));
        assertThat(batchError.getWrite(), sameInstance(write));
    }

    /**
     * Test method for
     * {@link BatchedNativeWriteCallback#BatchedNativeWriteCallback(Callback, BatchedWrite, AbstractMongoOperations, List)}
     * . This constructor is used for a set of write operations that do not use
     * the write commands. e.g., the server is pre-MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteFallbackToNativeStopOnFailure() {
        final Document doc = d().build();

        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).durability(Durability.NONE)
                .build();
        final AbstractMongoOperations mockCollection = createMock(AbstractMongoOperations.class);
        final List<WriteOperation> operations = write.getWrites();

        replay(mockResults, mockCollection);
        final BatchedNativeWriteCallback cb = new BatchedNativeWriteCallback(
                mockResults, write, mockCollection, operations);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockCollection);

        // Send the first request.
        reset(mockResults, mockCollection);
        mockCollection.insertAsync(anyObject(Callback.class), eq(true),
                eq(Durability.ACK), eq(doc));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.send();
        verify(mockResults, mockCollection);

        // Now the reply.
        reset(mockResults, mockCollection);
        mockCollection.updateAsync(anyObject(Callback.class), eq(doc), eq(doc),
                eq(false), eq(false), eq(Durability.ACK));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.callback(operations.get(0), 1);
        verify(mockResults, mockCollection);

        // Now the reply.
        reset(mockResults, mockCollection);
        mockCollection.deleteAsync(anyObject(Callback.class), eq(doc),
                eq(false), eq(Durability.ACK));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.callback(operations.get(1), 1);
        verify(mockResults, mockCollection);

        // Now the reply.
        reset(mockResults, mockCollection);
        mockResults.callback(Long.valueOf(3));
        expectLastCall();
        replay(mockResults, mockCollection);
        cb.callback(operations.get(2), 1);
        verify(mockResults, mockCollection);
    }
}
