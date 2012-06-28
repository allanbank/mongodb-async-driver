/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * MongoDatabaseImplTest provides tests for the {@link MongoDatabaseImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class MongoDatabaseImplTest {

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The instance under test. */
    private MongoDatabaseImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(Client.class);

        myTestInstance = new MongoDatabaseImpl(myMockClient, "test");
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
     * Test method for {@link MongoDatabaseImpl#drop()}.
     */
    @Test
    public void testDrop() {
        final Document goodResult = BuilderFactory.start().addDouble("ok", 1.0)
                .get();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .get();
        final Document missingOkResult = BuilderFactory.start().get();

        final Command command = new Command("test", BuilderFactory.start()
                .addInteger("dropDatabase", 1).get());

        myMockClient.send(eq(command), callback(reply(goodResult)));
        expectLastCall();
        myMockClient.send(eq(command), callback(reply(badResult)));
        expectLastCall();
        myMockClient.send(eq(command), callback(reply(missingOkResult)));
        expectLastCall();

        replay();

        assertTrue(myTestInstance.drop());
        assertFalse(myTestInstance.drop());
        assertFalse(myTestInstance.drop());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#getCollection(String)}.
     */
    @Test
    public void testGetCollection() {
        final MongoCollection collection = myTestInstance.getCollection("foo");
        assertTrue(collection instanceof MongoCollectionImpl);
        assertSame(myTestInstance,
                ((AbstractMongoCollection) collection).myDatabase);
        assertSame(myMockClient,
                ((AbstractMongoCollection) collection).myClient);
        assertEquals("foo", collection.getName());
    }

    /**
     * Test method for {@link MongoDatabaseImpl#listCollections()}.
     */
    @Test
    public void testListCollections() {

        final Document result1 = BuilderFactory.start()
                .addString("name", "test.collection").get();
        final Document result2 = BuilderFactory.start()
                .addString("name", "test.1.oplog.$").get();

        final Query query = new Query("test", "system.namespaces",
                BuilderFactory.start().get(), null, 0, 0, 0, false, true,
                false, false, false, false);

        myMockClient.send(eq(query), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        assertEquals(Arrays.asList("collection", "1.oplog.$"),
                myTestInstance.listCollections());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runAdminCommand(String)}.
     */
    @Test
    public void testRunAdminCommandString() {
        myTestInstance = new MongoDatabaseImpl(myMockClient, "admin");

        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("admin", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply, myTestInstance.runAdminCommand("command"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runAdminCommand(String, Document)}.
     */
    @Test
    public void testRunAdminCommandStringDocument() {
        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("admin", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();
        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply,
                myTestInstance.runAdminCommand("command", options.get()));
        assertSame(reply,
                myTestInstance.runAdminCommand("command", options.get()));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runAdminCommand(String, String, Document)}.
     */
    @Test
    public void testRunAdminCommandStringStringDocument() {
        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");

        final Command message = new Command("admin", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply,
                myTestInstance.runAdminCommand("command", "name", null));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String)}.
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentString() {
        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), anyObject(ReplyCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.runCommandAsync(mockCallback, "command");

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String, Document)}.
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentStringDocument() {

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);
        options.addString("command", "1");

        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), anyObject(ReplyCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.runCommandAsync(mockCallback, "command", options.get());

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String, String, Document)}
     * .
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentStringStringDocument() {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);
        options.addInteger("command", 1);

        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), anyObject(ReplyCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.runCommandAsync(mockCallback, "command", "name",
                options.get());

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runCommandAsync(String)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncString() throws Exception {

        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply, myTestInstance.runCommandAsync("command").get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(String, Document)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncStringDocument() throws Exception {
        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply,
                myTestInstance.runCommandAsync("command", options.get()).get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(String, String, Document)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncStringStringDocument() throws Exception {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply,
                myTestInstance
                        .runCommandAsync("command", "name", options.get())
                        .get());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runCommand(String)}.
     */
    @Test
    public void testRunCommandString() {
        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply, myTestInstance.runCommand("command"));

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runCommand(String, Document)}.
     */
    @Test
    public void testRunCommandStringDocument() {
        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command(myTestInstance.getName(),
                commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply, myTestInstance.runCommand("command", options.get()));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommand(String, String, Document)}.
     */
    @Test
    public void testRunCommandStringStringDocument() {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final Document reply = BuilderFactory.start().get();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.get());

        myMockClient.send(eq(message), callback(reply(reply)));
        expectLastCall();

        replay();

        assertSame(reply,
                myTestInstance.runCommand("command", "name", options.get()));

        verify();
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
