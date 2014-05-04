/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
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
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.state.Server;

/**
 * BatchedAsyncMongoCollectionImplTest provides tests for the
 * {@link BatchedAsyncMongoCollectionImpl} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedAsyncMongoCollectionImplTest {

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The parent database for the collection. */
    private MongoDatabase myMockDatabase = null;

    /** The stats for our fake cluster. */
    private ClusterStats myMockStats = null;

    /** The instance under test. */
    private BatchedAsyncMongoCollectionImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(SerialClientImpl.class);
        myMockDatabase = EasyMock.createMock(MongoDatabase.class);
        myMockStats = EasyMock.createMock(ClusterStats.class);

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
    public void testCloseNoBatchDeleteCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 1, 0);

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
        expect(myMockClient.getClusterStats()).andReturn(myMockStats);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_4, Version.VERSION_2_4));

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
    public void testCloseNoBatchUpdateCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 1, 0);

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
        expect(myMockClient.getClusterStats()).andReturn(myMockStats);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_4, Version.VERSION_2_4));

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
    public void testCloseWithBatchDeleteCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentAssignable deleteCommand = d(
                e("delete", "test"),
                e("ordered", false),
                e("writeConcern", d(e("w", 1))),
                e("deletes",
                        a(d(e("q", doc), e("limit", 0)),
                                d(e("q", doc), e("limit", 0)),
                                d(e("q", doc), e("limit", 0)))));
        final Command deleteMessage = new Command("test", "test",
                deleteCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

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
        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");
        myMockClient.send(eq(deleteMessage), callback(reply(replyDoc)));
        expectLastCall();
        replay();
        myTestInstance.setBatchDeletes(true);
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
    public void testCloseWithBatchUpdateCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentAssignable updateCommand = d(
                e("update", "test"),
                e("ordered", false),
                e("writeConcern", d(e("w", 1))),
                e("updates",
                        a(d(e("q", doc), e("u", update)),
                                d(e("q", doc), e("u", update)),
                                d(e("q", doc), e("u", update)))));
        final Command updateMessage = new Command("test", "test",
                updateCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

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
        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");
        myMockClient.send(eq(updateMessage), callback(reply(replyDoc)));
        expectLastCall();
        replay();
        myTestInstance.setBatchUpdates(true);
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
    public void testFlushNoBatchInsertCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 1, 0);

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
        expect(myMockClient.getClusterStats()).andReturn(myMockStats);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_4, Version.VERSION_2_4));

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
    public void testFlushWithBatchInsertCommand() throws InterruptedException,
            ExecutionException {
        final Document doc = BuilderFactory.start().add("_id", new ObjectId())
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final DocumentAssignable insertCommand = d(e("insert", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("documents", a(doc, doc, doc)));
        final Command insertMessage = new Command("test", "test",
                insertCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Integer> future2 = myTestInstance.insertAsync(doc);
        final Future<Integer> future3 = myTestInstance.insertAsync(doc);

        verify();

        reset();

        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");
        myMockClient.send(eq(insertMessage), callback(reply(replyDoc)));
        expectLastCall();

        replay();
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(1));
        assertThat(future2.get(), is(1));
        assertThat(future3.get(), is(1));
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
    public void testFlushWithBatchInterleavedCommands()
            throws InterruptedException, ExecutionException {
        final Document doc = BuilderFactory.start().add("_id", new ObjectId())
                .build();
        final Document update = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final DocumentAssignable insertCommand = d(e("insert", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("documents", a(doc)));
        final Command insertMessage = new Command("test", "test",
                insertCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        final Update updateMessage = new Update("test", "test", doc, update,
                false, false);
        final Delete deleteMessage = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 1, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);

        verify();

        reset();

        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(4);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE).times(3);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT).times(3);
        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(insertMessage), callback(reply(replyDoc)));
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

        assertThat(future1.get(), is(1)); // Note - fixed to Match the number of
        // documents.

        assertThat(future2.get(), is(2L));
        assertThat(future3.get(), is(2L));
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
    public void testFlushWithBatchInterleavedCommandsAndCanBatchDeleteWithMultipleDeletes()
            throws InterruptedException, ExecutionException {
        final Document doc = BuilderFactory.start().add("_id", new ObjectId())
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final DocumentAssignable insertCommand = d(e("insert", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("documents", a(doc)));
        final Command insertMessage = new Command("test", "test",
                insertCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        final DocumentAssignable deleteCommand = d(
                e("delete", "test"),
                e("ordered", false),
                e("writeConcern", d(e("w", 1))),
                e("deletes",
                        a(d(e("q", doc), e("limit", 0)),
                                d(e("q", doc), e("limit", 0)))));
        final Command deleteMessage = new Command("test", "test",
                deleteCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Long> future2 = myTestInstance.deleteAsync(doc);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);

        verify();

        reset();

        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(insertMessage), callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(deleteMessage), callback(reply(replyDoc)));
        expectLastCall();

        replay();
        myTestInstance.setBatchDeletes(true);
        myTestInstance.setBatchUpdates(true);
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(1)); // Note - fixed to Match the number of
        // documents.

        assertThat(future2.get(), is(2L));
        assertThat(future3.get(), is(2L));
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
    public void testFlushWithBatchInterleavedCommandsAndCanBatchUpdateAndDelete()
            throws InterruptedException, ExecutionException {
        final Document doc = BuilderFactory.start().add("_id", new ObjectId())
                .build();
        final Document update = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final DocumentAssignable insertCommand = d(e("insert", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("documents", a(doc)));
        final Command insertMessage = new Command("test", "test",
                insertCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        final DocumentAssignable updateCommand = d(e("update", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("updates", a(d(e("q", doc), e("u", update)))));
        final Command updateMessage = new Command("test", "test",
                updateCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        final DocumentAssignable deleteCommand = d(e("delete", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("deletes", a(d(e("q", doc), e("limit", 0)))));
        final Command deleteMessage = new Command("test", "test",
                deleteCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update);
        final Future<Long> future3 = myTestInstance.deleteAsync(doc);

        verify();

        reset();

        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(insertMessage), callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(updateMessage), callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(deleteMessage), callback(reply(replyDoc)));
        expectLastCall();

        replay();
        myTestInstance.setBatchDeletes(true);
        myTestInstance.setBatchUpdates(true);
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(1)); // Note - fixed to Match the number of
        // documents.

        assertThat(future2.get(), is(2L));
        assertThat(future3.get(), is(2L));
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
    public void testFlushWithBatchInterleavedCommandsAndCanBatchUpdateWithMultipleUpdates()
            throws InterruptedException, ExecutionException {
        final Document doc = BuilderFactory.start().add("_id", new ObjectId())
                .build();
        final Document update = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final DocumentAssignable insertCommand = d(e("insert", "test"),
                e("ordered", false), e("writeConcern", d(e("w", 1))),
                e("documents", a(doc)));
        final Command insertMessage = new Command("test", "test",
                insertCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        final DocumentAssignable updateCommand = d(
                e("update", "test"),
                e("ordered", false),
                e("writeConcern", d(e("w", 1))),
                e("updates",
                        a(d(e("q", doc), e("u", update)),
                                d(e("q", doc), e("u", update)))));
        final Command updateMessage = new Command("test", "test",
                updateCommand.asDocument(), ReadPreference.PRIMARY,
                VersionRange.minimum(BatchedWrite.REQUIRED_VERSION));

        expect(myMockDatabase.getName()).andReturn("test").times(6);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK).times(
                3);

        replay();

        final Future<Integer> future1 = myTestInstance.insertAsync(doc);
        final Future<Long> future2 = myTestInstance.updateAsync(doc, update);
        final Future<Long> future3 = myTestInstance.updateAsync(doc, update);

        verify();

        reset();

        expect(myMockClient.getClusterStats()).andReturn(myMockStats).times(2);
        expect(myMockStats.getServerVersionRange()).andReturn(
                VersionRange.range(Version.VERSION_2_6, Version.VERSION_2_6));
        expect(myMockStats.getSmallestMaxBsonObjectSize()).andReturn(
                (long) Client.MAX_DOCUMENT_SIZE);
        expect(myMockStats.getSmallestMaxBatchedWriteOperations()).andReturn(
                Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT);
        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(insertMessage), callback(reply(replyDoc)));
        expectLastCall();
        myMockClient.send(eq(updateMessage), callback(reply(replyDoc)));
        expectLastCall();

        replay();
        myTestInstance.setBatchDeletes(true);
        myTestInstance.setBatchUpdates(true);
        myTestInstance.flush();
        verify();

        assertThat(future1.get(), is(1)); // Note - fixed to match the number of
        // documents.

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
        EasyMock.replay(myMockClient, myMockDatabase, myMockStats);
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
        EasyMock.reset(myMockClient, myMockDatabase, myMockStats);
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
        EasyMock.verify(myMockClient, myMockDatabase, myMockStats);
    }

}
