/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;

/**
 * BatchedAsyncMongoCollectionImplTest provides TODO - Finish
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedAsyncMongoCollectionImplTest {

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The parent database for the collection. */
    private MongoDatabase myMockDatabase = null;

    /** The instance under test. */
    private BatchedAsyncMongoCollectionImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(SerialClientImpl.class);
        myMockDatabase = EasyMock.createMock(MongoDatabase.class);

        myTestInstance = new BatchedAsyncMongoCollectionImpl(myMockClient,
                myMockDatabase, "test");

        expect(myMockClient.getConfig()).andReturn(
                new MongoClientConfiguration()).anyTimes();
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockClient = null;
        myMockDatabase = null;

        myTestInstance = null;
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#cancel()}.
     * 
     * @throws InterruptedException
     *             On a test failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCancel() throws InterruptedException {
        final Document doc = BuilderFactory.start().build();

        final Callback<Long> mockCallback = createMock(Callback.class);

        expect(myMockDatabase.getName()).andReturn("test").times(8);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                4);

        replay(mockCallback);
        final Future<Long> future1 = myTestInstance.deleteAsync(doc);
        final Future<Long> future2 = myTestInstance.deleteAsync(doc);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);
        myTestInstance.deleteAsync(mockCallback, doc);
        verify(mockCallback);

        reset(mockCallback);
        // No sends.
        mockCallback.exception(anyObject(CancellationException.class));
        expectLastCall();

        replay(mockCallback);
        myTestInstance.cancel();
        myTestInstance.flush();
        verify(mockCallback);

        try {
            future1.get();
            fail("The future should have been cancelled.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause(),
                    instanceOf(CancellationException.class));
        }
        try {
            future2.get();
            fail("The future should have been cancelled.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause(),
                    instanceOf(CancellationException.class));
        }
        try {
            future3.get();
            fail("The future should have been cancelled.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause(),
                    instanceOf(CancellationException.class));
        }
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#close()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testCloseNoDeleteCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        expectLastCall();

        replay();
        final Future<Long> future1 = myTestInstance.deleteAsync(doc);
        final Future<Long> future2 = myTestInstance.deleteAsync(doc);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);
        verify();

        reset();
        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_4);
        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);
        replay();
        myTestInstance.close();
        verify();

        assertThat(future1.get(), is(1L));
        assertThat(future2.get(), is(1L));
        assertThat(future3.get(), is(1L));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#close()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testCloseNoUpdateCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);

        replay();
        final Future<Long> future1 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        final Future<Long> future3 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        verify();

        reset();
        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_4);
        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);
        replay();
        myTestInstance.close();
        verify();

        assertThat(future1.get(), is(1L));
        assertThat(future2.get(), is(1L));
        assertThat(future3.get(), is(1L));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#close()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testCloseWithDeleteCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        expectLastCall();

        replay();
        final Future<Long> future1 = myTestInstance.deleteAsync(doc);
        final Future<Long> future2 = myTestInstance.deleteAsync(doc);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);
        verify();

        reset();
        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_6);
        // TODO: Add delete command.
        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);
        replay();
        myTestInstance.close();
        verify();

        assertThat(future1.get(), is(1L));
        assertThat(future2.get(), is(1L));
        assertThat(future3.get(), is(1L));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#close()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testCloseWithUpdateCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);

        replay();
        final Future<Long> future1 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        final Future<Long> future3 = myTestInstance.updateAsync(doc, update,
                Durability.ACK);
        verify();

        reset();
        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_6);
        // TODO: Update with update command.
        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);
        replay();
        myTestInstance.close();
        verify();

        assertThat(future1.get(), is(1L));
        assertThat(future2.get(), is(1L));
        assertThat(future3.get(), is(1L));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#flush()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testFlushNoInsertCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Integer> future2 = myTestInstance.insertAsync(doc);
        final Future<Integer> future3 = myTestInstance.insertAsync(doc);

        verify();

        reset();

        // No batch command.
        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_4);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);

        replay();
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(2));
        assertThat(future2.get(), is(2));
        assertThat(future3.get(), is(2));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#flush()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testFlushWithInsertCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Integer> future2 = myTestInstance.insertAsync(doc);
        final Future<Integer> future3 = myTestInstance.insertAsync(doc);

        verify();

        reset();

        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_6);
        // TODO: Update with insert command.
        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall().times(3);

        replay();
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(2));
        assertThat(future2.get(), is(2));
        assertThat(future3.get(), is(2));
    }

    /**
     * Test method for {@link BatchedAsyncMongoCollectionImpl#flush()}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testFlushWithInterleavedCommands() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert insertMessage = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final Update updateMessage = new Update("test", "test", doc, update,
                false, false);
        final Delete deleteMessage = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);

        verify();

        reset();

        expect(myMockClient.getMinimumServerVersion()).andReturn(
                Version.VERSION_2_6);
        myMockClient.send(eq(insertMessage), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(updateMessage), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(deleteMessage), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(2));
        assertThat(future2.get(), is(2L));
        assertThat(future3.get(), is(2L));
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockClient} and {@link #myMockDatabase} objects.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockClient, myMockDatabase);
    }

    /**
     * Creates a reply around the document.
     * 
     * @param replyDoc
     *            The document to include in the reply.
     * @return The {@link Reply}
     */
    private Reply reply(final Document... replyDoc) {
        return new Reply(1, 0, 0, Arrays.asList(replyDoc), false, false, false,
                false);
    }

    /**
     * Performs a {@link EasyMock#reset(Object...)} on the provided mocks and
     * the {@link #myMockClient} and {@link #myMockDatabase} objects.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void reset(final Object... mocks) {
        EasyMock.reset(mocks);
        EasyMock.reset(myMockClient, myMockDatabase);
    }

    /**
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockClient} and {@link #myMockDatabase} objects.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockClient, myMockDatabase);
    }

}
