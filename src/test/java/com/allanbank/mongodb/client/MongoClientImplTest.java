/*
 * Copyright 2012-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.util.IOUtils;

/**
 * MongoClientImplTest provides tests for the {@link MongoClientImpl} class.
 * 
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientImplTest {

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The instance under test. */
    private MongoClientImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(Client.class);

        myTestInstance = new MongoClientImpl(myMockClient);
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockClient = null;

        myTestInstance = null;
    }

    /**
     * Test method for {@link MongoClientImpl#asSerializedClient()} .
     */
    @Test
    public void testAsSerializedClient() {
        final MongoClientImpl impl = new MongoClientImpl(
                new MongoClientConfiguration());
        assertThat(impl.getClient(), instanceOf(ClientImpl.class));
        impl.close();

        final MongoClient serial = impl.asSerializedClient();
        assertThat(serial, instanceOf(MongoClientImpl.class));
        final MongoClientImpl serialImpl = (MongoClientImpl) serial;
        assertThat(serialImpl.getClient(), instanceOf(SerialClientImpl.class));

        assertSame(serial, serial.asSerializedClient());
    }

    /**
     * Test method for {@link MongoClientImpl#close()}.
     */
    @Test
    public void testClose() {

        myMockClient.close();
        expectLastCall();

        replay();

        myTestInstance.close();

        verify();
    }

    /**
     * Test method for
     * {@link MongoClientImpl#MongoClientImpl(MongoClientConfiguration)} .
     */
    @Test
    public void testConstructor() {
        final MongoClientImpl impl = new MongoClientImpl(
                new MongoClientConfiguration());
        assertTrue(impl.getClient() instanceof ClientImpl);
        impl.close();
    }

    /**
     * Test method for {@link MongoClientImpl#getDatabase(java.lang.String)} .
     */
    @Test
    public void testGetDatabase() {
        final MongoDatabase database = myTestInstance.getDatabase("foo");
        assertTrue(database instanceof MongoDatabaseImpl);
        assertSame(myMockClient, ((MongoDatabaseImpl) database).myClient);
        assertEquals("foo", database.getName());

        assertThat(myTestInstance.getDatabase("foo"),
                Matchers.sameInstance(database));
    }

    /**
     * Test method for {@link MongoClientImpl#getDatabase(java.lang.String)} .
     */
    @Test
    public void testGetDatabaseCachingDoesRelease() {
        MongoDatabase database = myTestInstance.getDatabase("foo");
        assertThat(myTestInstance.getDatabase("foo"),
                Matchers.sameInstance(database));

        // Remember the instance id.
        final int instanceId = System.identityHashCode(database);

        database = null;

        // Flood the map/memory.
        final Random rand = new Random(System.currentTimeMillis());
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 10000000; ++i) {

            bytes = new byte[bytes.length + 1024];
            rand.nextBytes(bytes);

            final String name = IOUtils.toBase64(bytes);
            myTestInstance.getDatabase(name);

            if (((i % 10) == 0)
                    && (instanceId != System.identityHashCode(myTestInstance
                            .getDatabase("foo")))) {
                // Woot - got garbage collected.
                break;
            }

            // Try and nudge things along.
            System.gc();
        }

        database = myTestInstance.getDatabase("foo");
        assertThat(System.identityHashCode(database), not(is(instanceId)));
    }

    /**
     * Test method for {@link MongoClientImpl#getDatabase(java.lang.String)} .
     */
    @Test
    public void testGetDatabaseCachingDoesReleaseMaybeSeenViaGet() {
        MongoDatabase database = myTestInstance.getDatabase("foo");
        assertThat(myTestInstance.getDatabase("foo"),
                Matchers.sameInstance(database));

        // Remember the instance id.
        final int instanceId = System.identityHashCode(database);

        database = null;

        // Flood the map/memory.
        final Random rand = new Random(System.currentTimeMillis());
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 10000000; ++i) {

            bytes = new byte[bytes.length + 1024];
            rand.nextBytes(bytes);

            final String name = IOUtils.toBase64(bytes);
            myTestInstance.getDatabase(name);

            // Check ever time for the removed instance.
            if (instanceId != System.identityHashCode(myTestInstance
                    .getDatabase("foo"))) {
                // Woot - got garbage collected.
                break;
            }

            // Try and nudge things along.
            System.gc();
        }

        database = myTestInstance.getDatabase("foo");
        assertThat(System.identityHashCode(database), not(is(instanceId)));
    }

    /**
     * Test method for {@link MongoClientImpl#listDatabaseNames()}.
     */
    @Test
    public void testListDatabaseNames() {
        final DocumentBuilder reply = BuilderFactory.start();
        final ArrayBuilder dbEntry = reply.pushArray("databases");
        dbEntry.push().addString("name", "db_1");
        dbEntry.push().addString("name", "db_2");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("listDatabases", 1);

        final Command message = new Command("admin",
                Command.COMMAND_COLLECTION, commandDoc.build());

        expect(myMockClient.getConfig()).andReturn(
                new MongoClientConfiguration());
        myMockClient.send(eq(message), callback(reply(reply.build())));
        expectLastCall();

        replay();

        assertEquals(Arrays.asList("db_1", "db_2"),
                myTestInstance.listDatabaseNames());

        verify();
    }

    /**
     * Test method for {@link MongoClientImpl#listDatabases()}.
     */
    @Deprecated
    @Test
    public void testListDatabases() {
        final DocumentBuilder reply = BuilderFactory.start();
        final ArrayBuilder dbEntry = reply.pushArray("databases");
        dbEntry.push().addString("name", "db_1");
        dbEntry.push().addString("name", "db_2");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("listDatabases", 1);

        final Command message = new Command("admin",
                Command.COMMAND_COLLECTION, commandDoc.build());

        expect(myMockClient.getConfig()).andReturn(
                new MongoClientConfiguration());
        myMockClient.send(eq(message), callback(reply(reply.build())));
        expectLastCall();

        replay();

        assertEquals(Arrays.asList("db_1", "db_2"),
                myTestInstance.listDatabases());

        verify();
    }

    /**
     * Test method for {@link MongoClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testRestartDocumentAssignable() throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        expect(myMockClient.restart(b)).andReturn(null);

        replay();

        assertNull(myTestInstance.restart(b));

        verify();
    }

    /**
     * Test method for
     * {@link MongoClientImpl#restart(LambdaCallback,DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRestartLambdaCallbackDocumentAssignable()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final Capture<StreamCallback<Document>> capture = new Capture<StreamCallback<Document>>();

        expect(myMockClient.restart(capture(capture), eq(b))).andReturn(null);

        replay(mockCallback);

        assertNull(myTestInstance.restart(mockCallback, b));

        verify(mockCallback);

        assertThat(capture.getValue(), instanceOf(LambdaCallbackAdapter.class));
    }

    /**
     * Test method for
     * {@link MongoClientImpl#restart(StreamCallback,DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRestartStreamCallbackDocumentAssignable()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        expect(myMockClient.restart(mockCallback, b)).andReturn(null);

        replay(mockCallback);

        assertNull(myTestInstance.restart(mockCallback, b));

        verify(mockCallback);
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockClient} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockClient);
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
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockClient} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockClient);
    }
}
