/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.BatchedWriteException;

/**
 * BatchedWriteCallbackTest provides tests for the {@link BatchedWriteCallback}
 * class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteCallbackTest {

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,List, BatchedWrite, java.util.List)}
     * . This version is used when by the batching async collection interface.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteCallbackForAsyncInterface() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Reply> mockReplyCallback1 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback2 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback3 = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", Arrays.asList(mockReplyCallback1, mockReplyCallback2,
                        mockReplyCallback3), write, bundles);
        cb.setClient(mockClient);

        assertThat(cb.getForwardCallback(), nullValue());
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        cb.send();
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback1.callback(reply);
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture1.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback2.callback(reply);
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture2.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback3.callback(reply);
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture3.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,List, BatchedWrite, java.util.List)}
     * . This version is used when by the batching async collection interface.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteCallbackForAsyncInterfaceAndNullCallback() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Reply> replyCallback1 = null;
        final Callback<Reply> mockReplyCallback2 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback3 = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).durability(Durability.NONE)
                .build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockReplyCallback2, mockReplyCallback3, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", Arrays.asList(replyCallback1, mockReplyCallback2,
                        mockReplyCallback3), write, bundles);
        cb.setClient(mockClient);

        assertThat(cb.getForwardCallback(), nullValue());
        verify(mockReplyCallback2, mockReplyCallback3, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockReplyCallback2, mockReplyCallback3, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockReplyCallback2, mockReplyCallback3, mockClient);
        cb.send();
        verify(mockReplyCallback2, mockReplyCallback3, mockClient);

        // Now the results.
        reset(mockReplyCallback2, mockReplyCallback3, mockClient);
        replay(mockReplyCallback2, mockReplyCallback3, mockClient);
        capture1.getValue().callback(reply);
        verify(mockReplyCallback2, mockReplyCallback3, mockClient);

        // Now the results.
        reset(mockReplyCallback2, mockReplyCallback3, mockClient);
        mockReplyCallback2.callback(reply);
        expectLastCall();
        replay(mockReplyCallback2, mockReplyCallback3, mockClient);
        capture2.getValue().callback(reply);
        verify(mockReplyCallback2, mockReplyCallback3, mockClient);

        // Now the results.
        reset(mockReplyCallback2, mockReplyCallback3, mockClient);
        mockReplyCallback3.callback(reply);
        expectLastCall();
        replay(mockReplyCallback2, mockReplyCallback3, mockClient);
        capture3.getValue().callback(reply);
        verify(mockReplyCallback2, mockReplyCallback3, mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,List, BatchedWrite, java.util.List)}
     * . This version is used when by the batching async collection interface.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteCallbackForAsyncInterfaceInsufficientReplyCallbacks() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Reply> mockReplyCallback1 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback2 = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        replay(mockReplyCallback1, mockReplyCallback2, mockClient);
        try {
            final BatchedWriteCallback cb = new BatchedWriteCallback(
                    databaseName, "foo", Arrays.asList(mockReplyCallback1,
                            mockReplyCallback2), write, bundles);
            fail("Should not have created the callback with one less reply callback."
                    + cb);
        }
        catch (final IllegalArgumentException error) {
            // Good.
        }
        finally {
            verify(mockReplyCallback1, mockReplyCallback2, mockClient);
        }

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,List, BatchedWrite, java.util.List)}
     * . This version is used when by the batching async collection interface.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteCallbackForAsyncInterfaceWithWriteFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Reply> mockReplyCallback1 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback2 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback3 = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 0)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", Arrays.asList(mockReplyCallback1, mockReplyCallback2,
                        mockReplyCallback3), write, bundles);
        cb.setClient(mockClient);

        assertThat(cb.getForwardCallback(), nullValue());
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        cb.send();
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback1.callback(reply);
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture1.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback2.exception(anyObject(Throwable.class));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture2.getValue().callback(replyError);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback3.callback(reply);
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture3.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,List, BatchedWrite, java.util.List)}
     * . This version is used when by the batching async collection interface.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteCallbackForAsyncInterfaceWithWriteFailureAndStop() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Reply> mockReplyCallback1 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback2 = createMock(Callback.class);
        final Callback<Reply> mockReplyCallback3 = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 0)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", Arrays.asList(mockReplyCallback1, mockReplyCallback2,
                        mockReplyCallback3), write, bundles);
        cb.setClient(mockClient);

        assertThat(cb.getForwardCallback(), nullValue());
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        cb.send();
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback1.callback(reply);
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture1.getValue().callback(reply);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);

        // Now the results.
        reset(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        mockReplyCallback2.exception(anyObject(Throwable.class));
        mockReplyCallback3.exception(anyObject(Throwable.class));
        expectLastCall();
        replay(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
        capture2.getValue().callback(replyError);
        verify(mockReplyCallback1, mockReplyCallback2, mockReplyCallback3,
                mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommands() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        cb.send(); // Should do nothing.
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockResults.callback(Long.valueOf(3));
        expectLastCall();
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        capture2.getValue().callback(reply);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);

        assertThat(capture1.getValue().isLightWeight(), is(false));
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsDurabilityFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.push("writeConcernError").add("code", 1234)
                .add("errmsg", "Durabilty Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Durabilty Error"));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped().isEmpty(), is(true));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Throwable error = new Throwable("Something bad happened.");

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().exception(error);
        capture2.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(0)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Something bad happened."));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped().size(), is(0));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsLastReplyFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyErrorDoc = BuilderFactory.start().add("ok", 0)
                .add("errmsg", "Something bad happened.").build();
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc), false, false, false,
                false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        capture2.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(replyError);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(2)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Something bad happened."));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped().size(), is(0));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsReplyFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyErrorDoc = BuilderFactory.start().add("ok", 0)
                .add("errmsg", "Something bad happened.").build();
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc), false, false, false,
                false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(replyError);
        capture2.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(0)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Something bad happened."));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped().size(), is(0));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsStopOnDurabilityFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.push("writeConcernError").add("code", 1234)
                .add("errmsg", "Durabilty Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Durabilty Error"));
        assertThat(batchError.getN(), is(1L));
        assertThat(batchError.getSkipped(), is(write.getWrites().subList(2, 3)));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsStopOnFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class),
                anyObject(ReplyCallback.class));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class),
                anyObject(ReplyCallback.class));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.callback(bundles.get(0), reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class),
                anyObject(ReplyCallback.class));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.callback(bundles.get(1), reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockResults.callback(Long.valueOf(3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.callback(bundles.get(2), reply);
        verify(mockResults, mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsStopOnReplyFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final Document replyErrorDoc = BuilderFactory.start().add("ok", 0)
                .add("errmsg", "Something bad happened.").build();
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc), false, false, false,
                false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Something bad happened."));
        assertThat(batchError.getN(), is(1L));
        assertThat(batchError.getSkipped(), is(write.getWrites().subList(2, 3)));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsStopOnWriteFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 0)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Write Error"));
        assertThat(batchError.getN(), is(1L));
        assertThat(batchError.getSkipped(), is(write.getWrites().subList(2, 3)));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsStopOnWriteFailureAndSkip() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder()
                .mode(BatchedWriteMode.SERIALIZE_AND_STOP).insert(doc)
                .update(doc, doc).update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 0)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Write Error"));
        assertThat(batchError.getN(), is(1L));
        assertThat(batchError.getSkipped(), is(write.getWrites().subList(2, 4)));
        assertThat(batchError.getWrite(), sameInstance(write));

    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsWriteFailure() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 0)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        // Now the results.
        final Capture<Throwable> caughtError = new Capture<Throwable>();
        reset(mockResults, mockClient);
        mockResults.exception(capture(caughtError));
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);

        final Throwable caught = caughtError.getValue();
        assertThat(caught, instanceOf(BatchedWriteException.class));
        final BatchedWriteException batchError = (BatchedWriteException) caught;
        assertThat(batchError.getErrors().size(), is(1));
        assertThat(batchError.getErrors().keySet().iterator().next(), is(write
                .getWrites().get(1)));
        final Throwable t = batchError.getErrors().values().iterator().next();
        assertThat(t.getMessage(), is("Write Error"));
        assertThat(batchError.getN(), is(2L));
        assertThat(batchError.getSkipped().isEmpty(), is(true));
        assertThat(batchError.getWrite(), sameInstance(write));
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsWriteFailureWithBadOffset() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", 10)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockResults.callback(2L);
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);
    }

    /**
     * Test method for
     * {@link BatchedWriteCallback#BatchedWriteCallback(String, String,Callback, BatchedWrite, Client, List)}
     * . This constructor is used for a set of write operations that do use the
     * write commands. e.g., the server is on or after MongoDB 2.6.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBatchedWriteUsingWriteCommandsWriteFailureWithBadOffsetTooLow() {
        final Document doc = d().build();

        final String databaseName = "db";
        final Callback<Long> mockResults = createMock(Callback.class);
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final Client mockClient = createMock(Client.class);
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        final DocumentBuilder replyErrorDoc = BuilderFactory.start()
                .add("ok", 1).add("n", 0);
        replyErrorDoc.pushArray("writeErrors").push().add("index", -1)
                .add("code", 1234).add("errmsg", "Write Error");
        final Reply replyError = new Reply(0, 0, 0,
                Collections.singletonList(replyErrorDoc.build()), false, false,
                false, false);

        final Document replyDoc = BuilderFactory.start().add("ok", 1)
                .add("n", 1).build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        replay(mockResults, mockClient);
        final BatchedWriteCallback cb = new BatchedWriteCallback(databaseName,
                "foo", mockResults, write, mockClient, bundles);
        assertThat(cb.getForwardCallback(), sameInstance(mockResults));
        verify(mockResults, mockClient);

        final Capture<ReplyCallback> capture1 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture2 = new Capture<ReplyCallback>();
        final Capture<ReplyCallback> capture3 = new Capture<ReplyCallback>();

        // Send the requests.
        reset(mockResults, mockClient);
        mockClient.send(anyObject(Message.class), capture(capture1));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture2));
        expectLastCall();
        mockClient.send(anyObject(Message.class), capture(capture3));
        expectLastCall();
        replay(mockResults, mockClient);
        cb.send();
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture1.getValue().callback(reply);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        replay(mockResults, mockClient);
        capture2.getValue().callback(replyError);
        verify(mockResults, mockClient);

        // Now the results.
        reset(mockResults, mockClient);
        mockResults.callback(2L);
        expectLastCall();
        replay(mockResults, mockClient);
        capture3.getValue().callback(reply);
        verify(mockResults, mockClient);
    }
}
